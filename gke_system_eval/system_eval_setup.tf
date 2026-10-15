###############################################################################
# 0. TERRAFORM — the EVALUATION & DRIFT layer (the measurement system)
###############################################################################
# Implements the eval/drift design doc: the three measurement layers.
#
#   Offline eval harness   : KEDA ScaledJob on the Spot pool, consuming a
#                            Pub/Sub queue of candidates (published by the
#                            MLOps pipeline or CI). Runs golden suites,
#                            contamination check included.
#   Shadow scorer          : KEDA ScaledJob consuming the gateway's sampled
#                            shadow traffic; judges champion vs candidate,
#                            writes disagreements to the feedback queue.
#   Prod scorecard CronJob : the "watchtower" - daily golden-set runs
#                            AGAINST PRODUCTION; trend rows to BigQuery.
#   Telemetry              : eval_platform BigQuery dataset (scorecards,
#                            trends, judge calibration) + segment view.
#   Golden sets            : versioned GCS bucket, contamination-checked
#                            against the training stack's dataset manifests.
#
# Prereqs: serving stack (cluster, KEDA, spot pool), training stack
# (datasets bucket + manifests), MLOps stack (drift metrics table +
# feedback topic), platform stack (request_log ledger). Two-phase apply:
# KEDA CRDs must exist before the ScaledJob manifests can plan.
###############################################################################

terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.32"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

###############################################################################
# 1. VARIABLES + LOOKUPS
###############################################################################

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "cluster_name" {
  type    = string
  default = "model-serving"
}

variable "datasets_bucket" {
  description = "Training stack's datasets bucket - read for contamination manifests."
  type        = string
  default     = "ml-datasets"
}

variable "mlops_dataset" {
  description = "MLOps stack's dataset - drift metrics land in its drift_metrics table."
  type        = string
  default     = "ml_ops"
}

variable "feedback_topic" {
  description = "MLOps stack's feedback topic - shadow disagreements feed golden sets."
  type        = string
  default     = "model-feedback"
}

variable "judge_url" {
  description = "Judge model endpoint - frozen version per suite (design rule)."
  type        = string
  default     = "http://vllm-judge.model-serving.svc:80" # small model on the L4 pool
}

variable "prod_gateway_url" {
  description = "Production gateway Service - the watchtower scores against THIS."
  type        = string
  default     = "http://llm-gateway.model-serving.svc:80"
}

variable "eval_image" {
  type    = string
  default = "us-central1-docker.pkg.dev/REPLACE-PROJECT/model-serving/eval-harness:v0.1.0"
}

variable "shadow_image" {
  type    = string
  default = "us-central1-docker.pkg.dev/REPLACE-PROJECT/model-serving/shadow-scorer:v0.1.0"
}

variable "scorecard_image" {
  type    = string
  default = "us-central1-docker.pkg.dev/REPLACE-PROJECT/model-serving/prod-scorecard:v0.1.0"
}

locals {
  ns_eval  = "eval-harness"
  workload_pool = "${var.project_id}.svc.id.goog"
}

data "google_container_cluster" "existing" {
  name     = var.cluster_name
  location = var.region
}

# The training stack's datasets bucket (exists; we only need read access).
data "google_storage_bucket" "datasets" {
  name = "${var.datasets_bucket}-${var.project_id}"
}

###############################################################################
# 2. GOLDEN SETS — versioned bucket with a stable lineage
###############################################################################
# Golden sets are a PRODUCT (the design doc's rule): versioned snapshots
# with a hash, per-suite directories, and a contamination manifest stored
# BESIDE the set (list of training-snapshot hashes it must be disjoint from).
# The eval harness fails the run on any hash collision - the mechanical
# check that keeps evals from grading memorized homework.
###############################################################################

resource "google_storage_bucket" "golden_sets" {
  name                        = "golden-sets-${var.project_id}"
  location                    = var.region
  uniform_bucket_level_access = true

  # Every golden-set version is retained: re-baselining the champion after
  # a set update requires diffing against the previous set, and "roll back
  # the golden set" must be a copy, not archaeology.
  versioning {
    enabled = true
  }

  lifecycle {
    prevent_destroy = true
  }
}

###############################################################################
# 3. TELEMETRY — eval_platform BigQuery dataset
###############################################################################

resource "google_bigquery_dataset" "eval_platform" {
  dataset_id    = "eval_platform"
  friendly_name = "Evaluation & drift telemetry"
  location      = "US"
}

# --- Candidate scorecards: written by the offline harness (one row per
# candidate x suite). The MLOps promotion gate READS this - the contract.
resource "google_bigquery_table" "eval_scorecards" {
  dataset_id = google_bigquery_dataset.eval_platform.dataset_id
  table_id   = "eval_scorecards"

  schema = jsonencode([
    { name = "run_id",         type = "STRING",    mode = "REQUIRED" },
    { name = "candidate",     type = "STRING",    mode = "REQUIRED", description = "model URI / registry version under evaluation" },
    { name = "champion",      type = "STRING",    mode = "REQUIRED" },
    { name = "golden_set",    type = "STRING",    mode = "REQUIRED", description = "gs:// path + snapshot hash" },
    { name = "judge_version",  type = "STRING",    mode = "REQUIRED", description = "frozen judge per suite - scorecards without this are uninterpretable" },
    { name = "suite",         type = "STRING",    mode = "REQUIRED", description = "groundedness|instruction_following|safety|pairwise" },
    { name = "task_class",    type = "STRING",    mode = "REQUIRED" },
    { name = "score",         type = "FLOAT",     mode = "REQUIRED" },
    { name = "champion_score", type = "FLOAT",    mode = "NULLABLE" },
    { name = "passed",         type = "BOOL",      mode = "REQUIRED" },
    { name = "contamination_check", type = "BOOL", mode = "REQUIRED", description = "false => the whole run must fail closed" },
    { name = "evaluated_at",   type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# --- Watchtower trends: daily golden-set runs AGAINST PRODUCTION.
# Slow per-segment decay shows up here long before aggregates move.
resource "google_bigquery_table" "prod_scorecard_runs" {
  dataset_id = google_bigquery_dataset.eval_platform.dataset_id
  table_id   = "prod_scorecard_runs"

  schema = jsonencode([
    { name = "run_date",     type = "DATE",    mode = "REQUIRED" },
    { name = "suite",       type = "STRING",  mode = "REQUIRED" },
    { name = "task_class",  type = "STRING",  mode = "REQUIRED" },
    { name = "tenant_class", type = "STRING", mode = "REQUIRED" },
    { name = "score",       type = "FLOAT",   mode = "REQUIRED" },
    { name = "refusal_rate", type = "FLOAT",  mode = "NULLABLE", description = "tracked as a BAND, not a threshold (design rule)" },
    { name = "sample_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "judge_version", type = "STRING", mode = "REQUIRED" },
    { name = "gateway_version", type = "STRING", mode = "REQUIRED" },
  ])
}

# --- Judge calibration ledger: monthly human-vs-judge agreement runs.
# kappa < 0.7 means the judge suite needs re-baselining before its
# scorecards mean anything. Terraform creates the table; the calibration
# run itself is a one-off harness job (see outputs).
resource "google_bigquery_table" "judge_calibration" {
  dataset_id = google_bigquery_dataset.eval_platform.dataset_id
  table_id   = "judge_calibration"

  schema = jsonencode([
    { name = "calibrated_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "judge_version", type = "STRING",    mode = "REQUIRED" },
    { name = "sample_size",   type = "INTEGER",   mode = "REQUIRED" },
    { name = "agreement_rate", type = "FLOAT",    mode = "REQUIRED" },
    { name = "cohen_kappa",   type = "FLOAT",     mode = "REQUIRED" },
    { name = "verdict",       type = "STRING",    mode = "REQUIRED", description = "pass|rebaseline" },
  ])
}

# --- THE segment view: tenant x task x day. Averages hide the segment
# regression that matters (design doc anti-pattern #2) - this view is the
# one dashboards and drift checks should point at.
resource "google_bigquery_table" "v_segment_quality" {
  dataset_id = google_bigquery_dataset.eval_platform.dataset_id
  table_id   = "v_segment_quality"

  view {
    query = <<-SQL
      SELECT
        run_date,
        tenant_class,
        task_class,
        suite,
        AVG(score) AS avg_score,
        SUM(sample_count) AS samples,
        -- 7-day trailing delta: the "is this segment decaying?" number
        AVG(score) - FIRST_VALUE(AVG(score)) OVER (
          PARTITION BY tenant_class, task_class, suite
          ORDER BY run_date
          ROWS BETWEEN 6 PRECEDING AND 6 FOLLOWING
        ) AS trailing_week_delta
      FROM `${var.project_id}.${google_bigquery_dataset.eval_platform.dataset_id}.prod_scorecard_runs`
      GROUP BY run_date, tenant_class, task_class, suite
      ORDER BY run_date DESC
    SQL
    use_legacy_sql = false
  }
}

###############################################################################
# 4. PUB/SUB — candidate queue + shadow queue
###############################################################################

# --- Candidates to evaluate: published by the MLOps pipeline (post-train)
# or CI (prompt changes - prompts are code and regress models too).
resource "google_pubsub_topic" "eval_requests" {
  name = "eval-requests"
}

resource "google_pubsub_subscription" "eval_requests" {
  name  = "eval-harness-queue"
  topic = google_pubsub_topic.eval_requests.name

  ack_deadline_seconds = 600 # eval suites run minutes; long ack = no redelivery storms
  retry_policy {
    minimum_backoff = "30s"
    maximum_backoff = "600s"
  }
  expiration_policy {
    ttl = "" # never expire
  }
}

# --- Shadow traffic: the platform gateway publishes sampled
# (request, champion_response, candidate_response, context) triples here.
# Fire-and-forget on the gateway side; scored async here.
resource "google_pubsub_topic" "shadow_requests" {
  name = "shadow-requests"
}

resource "google_pubsub_subscription" "shadow_scorer_queue" {
  name  = "shadow-scorer-queue"
  topic = google_pubsub_topic.shadow_requests.name

  ack_deadline_seconds = 300
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }
  expiration_policy {
    ttl = ""
  }
}

###############################################################################
# 5. IAM — the eval identity
###############################################################################
# One SA for all eval workloads (harness, shadow scorer, scorecard CronJob):
# it reads golden sets + training manifests, writes scorecards, calls the
# judge and prod gateway, and feeds the feedback queue. No model-serving
# permissions, no training permissions - it measures, it does not touch.
###############################################################################

resource "google_service_account" "eval_runner" {
  account_id   = "eval-runner"
  display_name = "Evaluation harness + drift jobs"
}

# Golden sets: read + write (harness creates new candidate items; human
# review merges them - but the merge is also this identity, so it's auditable).
resource "google_storage_bucket_iam_member" "eval_golden_rw" {
  bucket = google_storage_bucket.golden_sets.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.eval_runner.email}"
}

# Training manifests: read-only, for the contamination check.
resource "google_storage_bucket_iam_member" "eval_read_datasets" {
  bucket = data.google_storage_bucket.datasets.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.eval_runner.email}"
}

# Scorecards + trends + calibration ledger.
resource "google_bigquery_dataset_iam_member" "eval_bq_write" {
  dataset_id = google_bigquery_dataset.eval_platform.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.eval_runner.email}"
}

# Drift metrics (embedding/prediction drift computed in the harness) land
# in the MLOps stack's table - the controller's change gate reads them.
resource "google_bigquery_dataset_iam_member" "eval_drift_write" {
  dataset_id = var.mlops_dataset
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.eval_runner.email}"
}

# Shadow disagreements -> the MLOps feedback queue (golden-set candidates).
resource "google_pubsub_topic_iam_member" "eval_publish_feedback" {
  topic  = var.feedback_topic
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.eval_runner.email}"
}

# Vertex AI: the judge may be a managed model; also Vector Search queries
# for the embedding-space drift job.
resource "google_project_iam_member" "eval_aiplatform" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.eval_runner.email}"
}

resource "google_service_account_iam_member" "wi_eval_runner" {
  service_account_id = google_service_account.eval_runner.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.workload_pool}[${local.ns_eval}/eval-runner]"
}

###############################################################################
# 6. KUBERNETES WIRING + NAMESPACE
###############################################################################

data "google_client_config" "default" {}

provider "kubernetes" {
  host  = "https://${data.google_container_cluster.existing.endpoint}"
  token = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(
    data.google_container_cluster.existing.master_auth[0].cluster_ca_certificate,
  )
}

resource "kubernetes_namespace" "eval" {
  metadata {
    name = local.ns_eval
  }
}

resource "kubernetes_service_account" "eval_runner" {
  metadata {
    name      = "eval-runner"
    namespace = kubernetes_namespace.eval.metadata[0].name
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.eval_runner.email
    }
  }
}

###############################################################################
# 7. OFFLINE EVAL HARNESS — KEDA ScaledJob on the Spot pool
###############################################################################
# Consumes eval-requests. Each message: one candidate x suite.
# Order of operations in the harness image (enforced, not optional):
#   1. Contamination check: hash golden items vs the manifest at
#      DATASET_MANIFEST_PREFIX. Collision => FAIL THE RUN, write nothing.
#   2. Score suite with the FROZEN judge (JUDGE_URL, JUDGE_VERSION from env).
#   3. Write scorecard rows to eval_scorecards; drift rows to drift_metrics.
#   4. Publish disagreements to the feedback topic.
###############################################################################

resource "kubernetes_manifest" "eval_harness_scaledjob" {
  manifest = {
    apiVersion = "keda.sh/v1alpha1"
    kind       = "ScaledJob"
    metadata = {
      name      = "eval-harness"
      namespace = kubernetes_namespace.eval.metadata[0].name
    }
    spec = {
      maxReplicaCount = 4 # concurrent suites; 300 items on one L4 = minutes
      pollingInterval = 30
      jobTargetRef = {
        backoffLimit = 1
        template = {
          spec = {
            restartPolicy = "Never"
            serviceAccountName = kubernetes_service_account.eval_runner.metadata[0].name
            containers = [
              {
                name  = "eval"
                image = var.eval_image

                env = [
                  { name = "GOLDEN_BUCKET", value = google_storage_bucket.golden_sets.name },
                  { name = "DATASET_MANIFEST_PREFIX", value = "${data.google_storage_bucket.datasets.name}/manifests" },
                  { name = "JUDGE_URL",     value = var.judge_url },
                  { name = "JUDGE_VERSION", value = "frozen-v3" }, # re-baseline on change, never silently
                  { name = "SCORECARD_TABLE", value = "${google_bigquery_dataset.eval_platform.dataset_id}.${google_bigquery_table.eval_scorecards.table_id}" },
                  { name = "DRIFT_TABLE",   value = "${var.mlops_dataset}.drift_metrics" },
                  { name = "FEEDBACK_TOPIC", value = var.feedback_topic },
                ]

                resources = {
                  # CPU-only: the judge runs on the L4 pool via HTTP; the
                  # harness orchestrates, it doesn't generate.
                  requests = { cpu = "2", memory = "4Gi" }
                  limits   = { cpu = "4", memory = "8Gi" }
                }
              }
            ]
            # Spot pool (serving stack): eval jobs are checkpoint-light and
            # re-runnable, so preemption costs a redelivery, not a result.
            nodeSelector = { pool = "llm-spot" }
            tolerations = [
              { key = "spot", operator = "Equal", value = "true", effect = "NoSchedule" },
              { key = "nvidia.com/gpu", operator = "Equal", value = "true", effect = "NoSchedule" },
            ]
          }
        }
      }
      triggers = [
        {
          type = "gcp-pubsub"
          metadata = {
            subscriptionName = google_pubsub_subscription.eval_requests.id
            mode             = "SubscriptionSize"
            value            = "1"
          }
        }
      ]
    }
  }
}

###############################################################################
# 8. SHADOW SCORER — consumes the gateway's sampled triples
###############################################################################
# Champion vs candidate, judged pairwise (more reliable than absolute
# scores - design rule). Disagreements flow to the feedback queue, which is
# the ONLY durable source of golden-set candidates.
###############################################################################

resource "kubernetes_manifest" "shadow_scorer_scaledjob" {
  manifest = {
    apiVersion = "keda.sh/v1alpha1"
    kind       = "ScaledJob"
    metadata = {
      name      = "shadow-scorer"
      namespace = kubernetes_namespace.eval.metadata[0].name
    }
    spec = {
      maxReplicaCount = 8
      pollingInterval = 15
      jobTargetRef = {
        backoffLimit = 2
        template = {
          spec = {
            restartPolicy = "Never"
            serviceAccountName = kubernetes_service_account.eval_runner.metadata[0].name
            containers = [
              {
                name  = "scorer"
                image = var.shadow_image
                env = [
                  { name = "JUDGE_URL",      value = var.judge_url },
                  { name = "JUDGE_VERSION",  value = "frozen-v3" },
                  { name = "FEEDBACK_TOPIC", value = var.feedback_topic },
                  { name = "SCORECARD_TABLE", value = "${google_bigquery_dataset.eval_platform.dataset_id}.${google_bigquery_table.eval_scorecards.table_id}" },
                ]
                resources = {
                  requests = { cpu = "1", memory = "2Gi" }
                  limits   = { cpu = "2", memory = "4Gi" }
                }
              }
            ]
            nodeSelector = { pool = "llm-spot" }
            tolerations = [
              { key = "spot", operator = "Equal", value = "true", effect = "NoSchedule" },
              { key = "nvidia.com/gpu", operator = "Equal", value = "true", effect = "NoSchedule" },
            ]
          }
        }
      }
      triggers = [
        {
          type = "gcp-pubsub"
          metadata = {
            subscriptionName = google_pubsub_subscription.shadow_scorer_queue.id
            mode             = "SubscriptionSize"
            value            = "5" # batch small: each message is one triple
          }
        }
      ]
    }
  }
}

###############################################################################
# 9. THE WATCHTOWER — daily golden-set runs AGAINST PRODUCTION
###############################################################################
# The highest-leverage habit in the design and the one teams skip: score
# prod daily, per segment, and watch the trend - not the average.
# Runs in-cluster (k8s CronJob) so it reaches the gateway Service via
# cluster-internal DNS with no LB/auth ceremony.
###############################################################################

resource "kubernetes_manifest" "prod_scorecard_cronjob" {
  manifest = {
    apiVersion = "batch/v1"
    kind       = "CronJob"
    metadata = {
      name      = "prod-scorecard"
      namespace = kubernetes_namespace.eval.metadata[0].name
    }
    spec = {
      # 04:00 UTC - after the serving stack's low-traffic window starts;
      # minutes of L4 time via the gateway, cost is noise.
      schedule = "0 4 * * *"
      concurrencyPolicy = "Forbid" # never two overlapping watchtower runs
      jobTemplate = {
        spec = {
          template = {
            spec = {
              restartPolicy  = "OnFailure"
              serviceAccountName = kubernetes_service_account.eval_runner.metadata[0].name
              containers = [
                {
                  name  = "scorecard"
                  image = var.scorecard_image
                  env = [
                    { name = "GOLDEN_BUCKET", value = google_storage_bucket.golden_sets.name },
                    # Score against PROD - this is the whole point of the watchtower.
                    { name = "GATEWAY_URL",  value = var.prod_gateway_url },
                    { name = "JUDGE_URL",     value = var.judge_url },
                    { name = "JUDGE_VERSION", value = "frozen-v3" },
                    { name = "TRENDS_TABLE",  value = "${google_bigquery_dataset.eval_platform.dataset_id}.${google_bigquery_table.prod_scorecard_runs.table_id}" },
                    # UPSTREAM-DRIFT NOTE: add a second schedule weekly that hits
                    # each PROVIDER directly (bypassing the gateway) with a fixed
                    # probe set - the canary-probe pattern from the design doc
                    # for providers swapping models under the same name.
                  ]
                  resources = {
                    requests = { cpu = "1", memory = "2Gi" }
                    limits   = { cpu = "2", memory = "4Gi" }
                  }
                }
              ]
              # Cheap CPU pod - the system pool, not Spot: it must run even
              # when the fleet is busy, reliability over pennies here.
              nodeSelector = { pool = "system" }
            }
          }
        }
      }
    }
  }
}

###############################################################################
# 10. OUTPUTS
###############################################################################

output "golden_sets_bucket" {
  description = "Land suites as versioned snapshots here; manifests beside them."
  value       = google_storage_bucket.golden_sets.url
}

output "eval_request_topic" {
  description = "Publish candidates here from the MLOps pipeline or CI (prompt changes count as model changes)."
  value       = google_pubsub_topic.eval_requests.id
}

output "shadow_topic" {
  description = "Point the platform gateway's shadow sampler at this topic (sample 5-10%, fire-and-forget)."
  value       = google_pubsub_topic.shadow_requests.id
}

output "segment_view" {
  description = "Dashboards + drift checks read v_segment_quality - never the aggregate."
  value       = "${google_bigquery_dataset.eval_platform.dataset_id}.${google_bigquery_table.v_segment_quality.table_id}"
}

output "calibration_note" {
  description = "The monthly human-agreement run."
  value       = "Run the harness with CALIBRATION=1 against ~100 human-verified pairs; write judge_calibration rows. kappa < 0.7 => re-baseline the judge suite before trusting any scorecard."
}
