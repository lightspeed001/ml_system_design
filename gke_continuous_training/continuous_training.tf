###############################################################################
# 0. TERRAFORM — the MLOps / CONTINUOUS-TRAINING control plane
###############################################################################
# Implements the MLOps design doc on top of the serving + training stacks.
# This stack is PURE GCP (no Kubernetes provider needed — the pipeline step
# that dispatches JobSets to Kueue authenticates from Vertex compute).
#
# Creates:
#   - Pub/Sub: trigger topic (all sources), feedback queue, approval queue
#   - Retraining controller: Cloud Run service (change/cooldown/budget gates)
#   - Cloud Scheduler: cadence trigger + drift-evaluation trigger
#   - Drift monitor: Cloud Run Job reading drift metrics from BigQuery
#   - BigQuery: ml_ops dataset (pipeline_runs, drift_metrics tables)
#   - Secret Manager: the kill switch
#   - SAs + IAM: controller, pipeline-runner (Vertex AI), scheduler, drift job
#
# Prereqs: the training stack's buckets (datasets/checkpoints) and the GKE
# cluster with Kueue exist. Update the IMAGE_* placeholders to your builds.
#
# First apply creates Cloud Run services + Pub/Sub wiring in one pass; there
# are no custom CRDs here, so no two-phase dance for once.
###############################################################################

terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

###############################################################################
# 1. VARIABLES
###############################################################################

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

# Cadence: weekly Monday 02:00 UTC — off-peak for the shared GPU fleet,
# and after the weekend's feedback curation lands in golden sets.
variable "retrain_schedule" {
  type    = string
  default = "0 2 * * 1"
}

# Drift evaluation: every 6h. Fast enough to catch shifts in hours
# (the design's SLO), cheap enough to run SQL + thresholds continuously.
variable "drift_schedule" {
  type    = string
  default = "0 */6 * * *"
}

variable "controller_image" {
  description = "Retraining controller image (the gates service)."
  type        = string
  default     = "us-central1-docker.pkg.dev/REPLACE-PROJECT/model-serving/retrain-controller:v0.1.0"
}

variable "drift_job_image" {
  description = "Drift evaluation job image (BigQuery SQL + threshold check)."
  type        = string
  default     = "us-central1-docker.pkg.dev/REPLACE-PROJECT/model-serving/drift-eval:v0.1.0"
}

variable "dataset_bucket" {
  description = "Datasets bucket from the training stack (golden sets live here)."
  type        = string
  default     = "ml-datasets"
}

variable "model_bucket" {
  description = "Model weights bucket from the serving stack (registry artifacts)."
  type        = string
  default     = "model-registry-weights"
}

variable "gke_cluster_name" {
  description = "Training cluster — pipeline steps dispatch JobSets to Kueue here."
  type        = string
  default     = "model-serving"
}

locals {
  ml_ops_dataset = "ml_ops"
}

###############################################################################
# 2. SECRET MANAGER — the kill switch
###############################################################################
# One flag that disables all automatic triggers and drains in-flight
# pipelines. The controller polls this on every trigger. Simple? Yes.
# And one day a data pipeline will corrupt a snapshot and you will be
# very glad "disable everything" is one secret flip, not a change review.
###############################################################################

resource "google_secret_manager_secret" "kill_switch" {
  secret_id = "retrain-kill-switch"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "kill_switch" {
  secret = google_secret_manager_secret.kill_switch.id
  # "off" = normal operation. Flip to "on" to disable all auto triggers.
  secret_data = "off"
}

###############################################################################
# 3. PUB/SUB — triggers in, feedback and approvals around
###############################################################################

# --- THE trigger topic: every source (scheduler, drift, snapshot commits,
# humans) publishes here; ONLY the controller consumes it. One funnel =
# one place where the gates are enforced.
resource "google_pubsub_topic" "retrain_triggers" {
  name = "retrain-triggers"

  message_retention_duration = "604800s"
}

# --- Feedback queue: production signals from the serving + agentic planes
# (thumbs, corrections, review outcomes) destined for golden-set curation.
resource "google_pubsub_topic" "model_feedback" {
  name = "model-feedback"
}

resource "google_pubsub_subscription" "feedback_review" {
  name  = "feedback-review"
  topic = google_pubsub_topic.model_feedback.name

  # Human-curated: messages wait for the review UI, not for a pod.
  ack_deadline_seconds    = 600
  expiration_policy {
    ttl = "" # never expire
  }
  retention_duration = "604800s" # 7d
}

# --- Approval queue: controller publishes approval requests when a
# pipeline version requires named human sign-off (new base model, new
# data source, post-incident). Your review UI subscribes.
resource "google_pubsub_topic" "approval_requests" {
  name = "model-approval-requests"
}

###############################################################################
# 4. BIGQUERY — the flat, queryable half of lineage
###############################################################################
# ML Metadata holds the run graph; THIS dataset is the copy you actually
# query ("why is v42 live?", "GPU hours this cycle?", "drift trend?").
###############################################################################

resource "google_bigquery_dataset" "ml_ops" {
  dataset_id    = local.ml_ops_dataset
  friendly_name = "MLOps control plane"
  location      = "US"

  # Long retention: lineage queries outlive any individual model.
  default_table_expiration = null # never expire
}

# --- pipeline_runs: one row per pipeline run, the controller's ledger.
# Cooldown windows and budget caps are DERIVED from this table —
# stateless controller + append-only ledger = no drift, no lost state.
resource "google_bigquery_table" "pipeline_runs" {
  dataset_id = google_bigquery_dataset.ml_ops.dataset_id
  table_id   = "pipeline_runs"

  schema = jsonencode([
    { name = "run_id",          type = "STRING", mode = "REQUIRED", description = "pipeline run id (matches ML Metadata run)" },
    { name = "trigger",         type = "STRING", mode = "REQUIRED", description = "cadence | drift | snapshot | manual | incident" },
    { name = "trigger_detail",  type = "JSON",   mode = "NULLABLE" },
    { name = "model_name",      type = "STRING", mode = "REQUIRED" },
    { name = "dataset_hash",    type = "STRING", mode = "REQUIRED", description = "immutable snapshot hash — the lineage anchor" },
    { name = "code_sha",        type = "STRING", mode = "REQUIRED" },
    { name = "status",          type = "STRING", mode = "REQUIRED", description = "launched|suppressed|training|evaluating|registered|promoted|aborted" },
    { name = "gate_detail",    type = "JSON",   mode = "NULLABLE", description = "which gate fired and why (auditable decisions)" },
    { name = "eval_scorecard",  type = "JSON",   mode = "NULLABLE" },
    { name = "gpu_hours",       type = "FLOAT",  mode = "NULLABLE" },
    { name = "started_at",      type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "ended_at",        type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

# --- drift_metrics: written by the drift-eval job (section 7); the
# controller's CHANGE gate and the monitors' trend views both read it.
resource "google_bigquery_table" "drift_metrics" {
  dataset_id = google_bigquery_dataset.ml_ops.dataset_id
  table_id   = "drift_metrics"

  schema = jsonencode([
    { name = "computed_at",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "model_name",    type = "STRING",    mode = "REQUIRED" },
    { name = "metric_name",   type = "STRING",    mode = "REQUIRED", description = "e.g. topic_mix_psi, eval_golden_score, refusal_rate" },
    { name = "metric_value",  type = "FLOAT",     mode = "REQUIRED" },
    { name = "threshold",     type = "FLOAT",     mode = "REQUIRED" },
    { name = "drifted",       type = "BOOL",      mode = "REQUIRED" },
    { name = "window_start",  type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_end",    type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

###############################################################################
# 5. SERVICE ACCOUNTS — least privilege per automation
###############################################################################

# --- Controller: the ONLY thing allowed to start a pipeline.
resource "google_service_account" "controller" {
  account_id   = "retrain-controller"
  display_name = "Retraining controller (gates + pipeline launch)"
}

# --- Pipeline runner: the SA Vertex AI Pipelines steps execute as.
# This is the identity that dispatches JobSets to Kueue on the GKE fleet
# ("dispatch and wait" pattern from the design doc).
resource "google_service_account" "pipeline_runner" {
  account_id   = "ml-pipeline-runner"
  display_name = "Vertex AI Pipeline steps (train/eval/register)"
}

# --- Scheduler + drift job identities.
resource "google_service_account" "scheduler" {
  account_id   = "ml-ops-scheduler"
  display_name = "Cloud Scheduler triggers"
}

resource "google_service_account" "drift_eval" {
  account_id   = "drift-eval"
  display_name = "Drift evaluation job"
}

# Controller grants:
#   - launch pipelines + read Model Registry  -> aiplatform.user
#   - read/write the run ledger              -> bigquery.dataEditor on ml_ops
#   - read the kill switch                   -> secretmanager accessor
#   - publish approval + trigger messages    -> pubsub publisher
#   - query serving/agent run logs for budget -> bigquery.dataViewer (comment)
resource "google_project_iam_member" "controller_aiplatform" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_bigquery_dataset_iam_member" "controller_bq" {
  dataset_id = google_bigquery_dataset.ml_ops.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_secret_manager_secret_iam_member" "controller_kill_switch" {
  secret_id = google_secret_manager_secret.kill_switch.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.controller.email}"
}

resource "google_pubsub_topic_iam_member" "controller_publish" {
  for_each = {
    approvals = google_pubsub_topic.approval_requests.name
    triggers  = google_pubsub_topic.retrain_triggers.name # manual re-queues
  }
  topic  = each.value
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.controller.email}"
}

# Pipeline runner grants:
#   - Vertex AI Pipelines execution (training jobs dispatch, eval, registry)
#   - read datasets + write model artifacts to the serving/training buckets
#   - dispatch-and-wait to Kueue: container.developer on the training cluster.
#     (Project-level is the practical choice; if your org allows only
#     cluster-scoped grants, scope this tighter.)
resource "google_project_iam_member" "pipeline_runner_aiplatform" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_project_iam_member" "pipeline_runner_artifacts" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_project_iam_member" "pipeline_runner_gke_dispatch" {
  project = var.project_id
  role    = "roles/container.developer"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

# Drift eval: reads BigQuery, publishes drift triggers.
resource "google_bigquery_dataset_iam_member" "drift_eval_bq" {
  dataset_id = google_bigquery_dataset.ml_ops.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.drift_eval.email}"
}

resource "google_pubsub_topic_iam_member" "drift_eval_publish" {
  topic  = google_pubsub_topic.retrain_triggers.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.drift_eval.email}"
}

# Scheduler: publishes cadence triggers.
resource "google_pubsub_topic_iam_member" "scheduler_publish" {
  topic  = google_pubsub_topic.retrain_triggers.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.scheduler.email}"
}

###############################################################################
# 6. THE CONTROLLER — Cloud Run service, push-triggered from Pub/Sub
###############################################################################

resource "google_cloud_run_v2_service" "controller" {
  name     = "retrain-controller"
  location = var.region

  template {
    service_account = google_service_account.controller.email

    containers = [
      {
        image = var.controller_image

        env = [
          { name = "PROJECT_ID",      value = var.project_id },
          { name = "TRIGGER_TOPIC",   value = google_pubsub_topic.retrain_triggers.name },
          { name = "APPROVAL_TOPIC",  value = google_pubsub_topic.approval_requests.name },
          { name = "KILL_SWITCH",    value = google_secret_manager_secret.kill_switch.secret_id },
          { name = "RUN_LEDGER",     value = "${google_bigquery_table.pipeline_runs.dataset_id}.${google_bigquery_table.pipeline_runs.table_id}" },
          { name = "GKE_CLUSTER",    value = var.gke_cluster_name },
          { name = "GKE_LOCATION",   value = var.region },
          # Policy lives in git, but the controller caches the parsed
          # policy file — change gates, cooldowns, budget caps all read
          # from here. "Someone changed a threshold in a console" is an
          # incident class; this makes it impossible.
          { name = "POLICY_URI",     value = "gs://${var.dataset_bucket}/policy/retrain-policy.yaml" },
        ]

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        startup_probe {
          http_get = { path = "/healthz" }
        }
      }
    ]
  }
}

# Controller ingress: Pub/Sub push arrives from Google's network. Public
# ingress + OIDC is the common pattern; if your org requires private-only,
# use a serverless VPC / PSC endpoint instead of relaxing auth.
resource "google_cloud_run_v2_service_iam_member" "pubsub_push_can_invoke" {
  name     = google_cloud_run_v2_service.controller.name
  location = var.region
  role      = "roles/run.invoker"
  member    = "serviceAccount:${google_service_account.controller.email}"
}

# The funnel: everything that can start a retrain lands on this push
# subscription, and the controller's three gates (change, cooldown,
# budget) run on every message.
resource "google_pubsub_subscription" "controller_triggers" {
  name  = "controller-triggers"
  topic = google_pubsub_topic.retrain_triggers.name

  # Push to the controller with an OIDC token minted for its SA —
  # the endpoint rejects anything without a valid token.
  push_config {
    push_endpoint = "${google_cloud_run_v2_service.controller.uri}/triggers"

    oidc_token {
      service_account_email = google_service_account.controller.email
      audience              = google_cloud_run_v2_service.controller.uri
    }
  }

  ack_deadline_seconds = 60

  # The gates are fast (SQL + secret read); one retry then DLQ.
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "60s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.retrain_triggers.name # change to a real DLQ topic in prod
    max_delivery_attempts = 5
  }
}

###############################################################################
# 7. DRIFT MONITOR — scheduled Cloud Run Job
###############################################################################
# Runs the drift SQL against serving/agent run logs, writes drift_metrics,
# and publishes a trigger ONLY when a metric crosses its threshold.
# For LLMs these are topic-mix PSI, golden-set score trends, refusal and
# safety-signal rates — the no-labels monitoring from the design doc.
###############################################################################

resource "google_cloud_run_v2_job" "drift_eval" {
  name     = "drift-eval"
  location = var.region

  template {
    template {
      service_account = google_service_account.drift_eval.email

      containers = [
        {
          image = var.drift_job_image

          env = [
            { name = "PROJECT_ID",    value = var.project_id },
            { name = "METRICS_TABLE", value = "${google_bigquery_table.drift_metrics.dataset_id}.${google_bigquery_table.drift_metrics.table_id}" },
            { name = "TRIGGER_TOPIC", value = google_pubsub_topic.retrain_triggers.name },
          ]

          resources {
            limits = {
              cpu    = "1"
              memory = "512Mi"
            }
          }
        }
      ]

      # Drift runs are idempotent and cheap; fail loudly, retry a bit.
      timeout = "600s"
    }
  }
}

# Scheduler may execute the drift job.
resource "google_cloud_run_v2_job_iam_member" "scheduler_run_drift" {
  name     = google_cloud_run_v2_job.drift_eval.name
  location = var.region
  role      = "roles/run.invoker"
  member    = "serviceAccount:${google_service_account.scheduler.email}"
}

###############################################################################
# 8. CLOUD SCHEDULER — cadence + drift evaluation
###############################################################################

# --- Cadence: the weekly "should we retrain?" poke. It is a REQUEST to
# the controller, not an instruction — the change gate may still suppress
# it if the candidate isn't worth GPU-hours.
resource "google_cloud_scheduler_job" "cadence" {
  name             = "retrain-cadence"
  region           = var.region
  schedule         = var.retrain_schedule
  time_zone        = "UTC"

  pubsub_target {
    # topic resource name
    topic_name = google_pubsub_topic.retrain_triggers.id

    # The controller parses this payload; the policy in git decides what
    # "cadence" means for each (model, dataset) pair.
    data = base64encode(jsonencode({
      trigger      = "cadence"
      model_name   = "mistral-7b-instruct"
      requested_by = "cloud-scheduler"
    }))

    attributes = {
      trigger-type = "cadence"
    }
  }
}

# --- Drift evaluation on its own cadence (more frequent than retraining).
resource "google_cloud_scheduler_job" "drift_eval" {
  name             = "drift-eval"
  region           = var.region
  schedule         = var.drift_schedule
  time_zone        = "UTC"

  http_target {
    # Executing the Cloud Run Job directly (OIDC as the scheduler SA).
    uri         = "https://${var.region}-run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.drift_eval.name}:run"
    http_method = "POST"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}

# Scheduler SAs need to exist on the jobs above; grant the scheduler SA
# itself permission to mint tokens (the project-level service agent is
# normally already present, but be explicit).
resource "google_project_iam_member" "scheduler_token" {
  project = var.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:${google_service_account.scheduler.email}"
}

###############################################################################
# 9. OUTPUTS
###############################################################################

output "controller_uri" {
  description = "Retraining controller endpoint (receives Pub/Sub push triggers)."
  value       = google_cloud_run_v2_service.controller.uri
}

output "trigger_topic" {
  description = "Publish here from CI (snapshot commits) or humans (incident) — the controller gates everything."
  value       = google_pubsub_topic.retrain_triggers.id
}

output "feedback_subscription" {
  description = "Wire the review UI here — curated feedback becomes golden sets."
  value       = google_pubsub_subscription.feedback_review.id
}

output "ledger_table" {
  description = "The run ledger — cooldowns, budgets, and lineage queries derive from it."
  value       = "${google_bigquery_table.pipeline_runs.dataset_id}.${google_bigquery_table.pipeline_runs.table_id}"
}

output "kill_switch_note" {
  description = "Emergency procedure."
  value       = "Flip secret 'retrain-kill-switch' to 'on' (Secret Manager) — all automatic triggers stop at the next message; in-flight pipelines drain per policy. Flip back after the postmortem."
}
