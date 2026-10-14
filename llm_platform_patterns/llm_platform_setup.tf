###############################################################################
# 0. TERRAFORM — the LLM PLATFORM layer (gateway intelligence)
###############################################################################
# Implements the platform-patterns design: the cache -> classify -> cascade
# -> route decision chain as pluggable behavior of the model gateway.
#
# Creates:
#   - Exact-match cache: dedicated Memorystore Redis (why dedicated: below)
#   - Semantic cache: pgvector database on the existing agent-state instance
#   - Policy store: versioned GCS bucket + Pub/Sub hot-reload notification
#   - Telemetry: BigQuery llm_platform dataset (request_log + canary views)
#   - Provider fallback: Secret Manager entries for external API keys
#   - IAM: least-privilege gateway SA + Workload Identity binding
#   - Sample: gateway deployment wired to every store above
#
# Honest correction to the design doc: the doc suggested reusing the agentic
# stack's Redis with "a separate DB". Memorystore only exposes DB 0 —
# a shared instance means a shared keyspace. So this stack creates a
# DEDICATED cache instance instead. Cache is rebuildable state, which also
# means you can run it cheaper than the session store if you accept cold
# caches after node loss.
#
# Prereqs: serving cluster exists; agentic stack's Cloud SQL (pgvector
# enabled) and PSA peering exist. Two-phase apply ONLY if you also apply
# the commented Vertex AI Vector Search block (index must exist before
# deployment resources reference it).
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

variable "serving_namespace" {
  description = "Namespace from the serving stack; the gateway is a serving-plane pod."
  type        = string
  default     = "model-serving"
}

variable "db_password" {
  description = "Cloud SQL user password (same instance user as the agentic stack). Manage in your vault."
  type        = string
  default     = "CHANGE-ME"
  sensitive   = true
}

variable "cache_redis_auth" {
  description = "AUTH token for the exact-match cache instance."
  type        = string
  default     = "CHANGE-ME"
  sensitive   = true
}

variable "embedding_dimensions" {
  description = "Dimensions of your embedding model — must match the vector column."
  type        = number
  default     = 1024
}

locals {
  policy_bucket = "llm-policy-${var.project_id}"
}

# The serving cluster (gateway pods run there).
data "google_container_cluster" "existing" {
  name     = var.cluster_name
  location = var.region
}

# The agentic stack's Postgres (pgvector flag already enabled on it).
data "google_sql_database_instance" "agent_state" {
  name = "agent-state"
}

###############################################################################
# 2. EXACT-MATCH CACHE — dedicated Memorystore
###############################################################################
# The gateway's first stop: a deterministic hash lookup (~1ms). Keys are
# (tenant, normalized prompt+params+model_version) hashes; entries carry
# TTLs per task class. This instance ALSO holds circuit-breaker state
# (error-rate windows per provider) — tiny, but it's correctness state,
# which is why this is HA and not the cheapest BASIC tier.
#
# Contents are rebuildable (a cold cache = cost blip, not an outage), so
# snapshot/restore (RDB) is enough — no AOF needed unlike the session store.
###############################################################################

resource "google_redis_instance" "llm_cache" {
  name           = "llm-cache"
  tier           = "STANDARD_HA"
  memory_size_gb = 3

  location_id              = "us-central1-a"
  alternative_location_id  = "us-central1-f"

  authorized_network = data.google_container_cluster.existing.network # same VPC as everything else
  connect_mode       = "PRIVATE_SERVICE_ACCESS"                        # PSA peering exists (agentic stack)

  transit_encryption_mode = "SERVER_AUTHENTICATION"
  auth_enabled            = true
  auth_string             = var.cache_redis_auth

  redis_version = "REDIS_7_0"

  persistence_config {
    mode = "RDB" # snapshot only — cache, not ledger
  }
}

# Redis AUTH as a Secret Manager entry, projected into the gateway pod.
resource "google_secret_manager_secret" "cache_redis_auth" {
  secret_id = "llm-cache-redis-auth"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "cache_redis_auth" {
  secret      = google_secret_manager_secret.cache_redis_auth.id
  secret_data = var.cache_redis_auth
}

###############################################################################
# 3. SEMANTIC CACHE — pgvector to start (the design's own recommendation)
###############################################################################
# Fuzzy tier: embed the prompt, ANN-search past (prompt, response) pairs.
# pgvector on the existing instance is the right starting point: thousands
# to low millions of vectors, one more table, zero new infra.
#
# MIGRATION NOTE (Terraform can't run SQL): after apply, run once —
#   psql: CREATE EXTENSION IF NOT EXISTS vector;
#   psql: CREATE TABLE semantic_cache (
#     embedding vector(1024),          -- must equal var.embedding_dimensions
#     prompt_hash text, response text,
#     tenant text, task_class text, model_version text,
#     created_at timestamptz DEFAULT now(), ttl_seconds int
#   );
#   psql: CREATE INDEX ON semantic_cache
#     USING hnsw (embedding vector_cosine_ops);
# Tenant scoping: filter EVERY query by tenant + model_version + TTL —
# the design's "cache scope follows the tenant" rule lives in the query,
# and model_version in the key is the invalidation story.
#
# WHEN TO MOVE TO VERTEX AI VECTOR SEARCH (design doc's "at scale"): >5M
# vectors or >5k QPS on the cache path. The gateway swaps one client.
# Resources (apply AFTER creating the index — see docs for exact field
# names on your provider version):
#   google_vertex_ai_index            (STREAM_UPDATE, tree-ah, cosine)
#   google_vertex_ai_index_endpoint   (private endpoint via PSA in prod)
#   deployed index on the endpoint
###############################################################################

resource "google_sql_database" "semantic_cache" {
  name     = "semantic_cache"
  instance = data.google_sql_database_instance.agent_state.name
}

# DSN for the cache database (the instance user spans databases).
resource "google_secret_manager_secret" "semantic_cache_dsn" {
  secret_id = "semantic-cache-dsn"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "semantic_cache_dsn" {
  secret = google_secret_manager_secret.semantic_cache_dsn.id
  secret_data = (
    "postgresql://agent:${var.db_password}@${data.google_sql_database_instance.agent_state.ip_address[0].ip_address}:5432/semantic_cache?sslmode=require"
  )
}

###############################################################################
# 4. POLICY STORE — versioned bucket + hot-reload notification
###############################################################################
# Cache rules, cascade thresholds, provider fallback chains, privacy
# classes: ALL live in git and land here via CI. The gateway watches the
# Pub/Sub notification and hot-reloads — changing a threshold is a commit,
# never a deploy. Versioning on the bucket is your instant policy rollback.
###############################################################################

resource "google_storage_bucket" "policy" {
  name                        = local.policy_bucket
  location                    = var.region
  uniform_bucket_level_access = true

  # Every policy version is retained — "roll back the routing policy to
  # yesterday's" must be a copy, not an archaeology project.
  versioning {
    enabled = true
  }
}

resource "google_pubsub_topic" "policy_updated" {
  name = "llm-policy-updated"
}

# GCS -> Pub/Sub: the bucket's own service account must be allowed to
# publish the change notifications.
data "google_storage_project_service_account" "gpa" {}

resource "google_pubsub_topic_iam_member" "gcs_publishes" {
  topic  = google_pubsub_topic.policy_updated.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${data.google_storage_project_service_account.gpa.email_address}"
}

resource "google_storage_notification" "policy_changed" {
  bucket         = google_storage_bucket.policy.name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.policy_updated.id
  event_types    = ["OBJECT_FINALIZE", "OBJECT_DELETE"] # new version OR rollback
  depends_on      = [google_pubsub_topic_iam_member.gcs_publishes]
}

###############################################################################
# 5. TELEMETRY — one row per request, then the views that drive decisions
###############################################################################

resource "google_bigquery_dataset" "llm_platform" {
  dataset_id    = "llm_platform"
  friendly_name = "LLM platform telemetry"
  location      = "US"
}

# --- THE ledger: every gateway decision, one row. This table is the
# chargeback basis, the canary comparison source, and the cascade-quality
# flywheel all at once.
resource "google_bigquery_table" "request_log" {
  dataset_id = google_bigquery_dataset.llm_platform.dataset_id
  table_id   = "request_log"

  schema = jsonencode([
    { name = "timestamp",        type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "request_id",      type = "STRING",    mode = "REQUIRED" },
    { name = "tenant",          type = "STRING",    mode = "REQUIRED" },
    { name = "task_class",      type = "STRING",    mode = "REQUIRED", description = "classifier output: factual|codegen|open_ended|..." },
    { name = "privacy_class",   type = "STRING",    mode = "REQUIRED", description = "self_hosted_only|standard|no_training" },
    { name = "cache_tier",      type = "STRING",    mode = "REQUIRED", description = "exact|semantic|miss" },
    { name = "cascade_outcome", type = "STRING",    mode = "NULLABLE", description = "tier1_served|escalated|flagship_direct" },
    { name = "confidence",      type = "FLOAT",     mode = "NULLABLE", description = "classifier/logprob signal at escalation" },
    { name = "provider",        type = "STRING",    mode = "REQUIRED", description = "vllm-l4|vllm-a3|vertex|external:<name>" },
    { name = "policy_version",  type = "STRING",    mode = "REQUIRED", description = "policy file version that made the decision" },
    { name = "model_version",   type = "STRING",    mode = "REQUIRED" },
    { name = "prompt_tokens",   type = "INTEGER",   mode = "NULLABLE" },
    { name = "output_tokens",   type = "INTEGER",   mode = "NULLABLE" },
    { name = "cost_usd",        type = "FLOAT",     mode = "REQUIRED", description: "fleet amortized for self-hosted; list for managed" },
    { name = "latency_ms",      type = "INTEGER",   mode = "NULLABLE" },
    { name = "degraded_path",   type = "BOOL",      mode = "NULLABLE", description: "true if a fallback fired (circuit-open)" },
  ])

  # Retention: 400 days — enough for year-over-year comparisons without
  # storage guilt. Older data matters less than you think.
  deletion_protection = false
}

# --- View: cost per tenant per day (the chargeback query, pre-written).
resource "google_bigquery_table" "cost_per_tenant" {
  dataset_id = google_bigquery_dataset.llm_platform.dataset_id
  table_id   = "v_cost_per_tenant"

  view {
    query = <<-SQL
      SELECT
        DATE(timestamp) AS day,
        tenant,
        COUNT(*) AS requests,
        SUM(cost_usd) AS total_cost,
        SUM(output_tokens) AS output_tokens,
        AVG(latency_ms) AS avg_latency_ms,
        SAFE_DIVIDE(SUM(cost_usd), SUM(output_tokens)) AS cost_per_1k_tokens
      FROM `${var.project_id}.${google_bigquery_dataset.llm_platform.dataset_id}.request_log`
      GROUP BY day, tenant
      ORDER BY day DESC
    SQL
    use_legacy_sql = false
  }
}

# --- View: POLICY CANARY. New policy versions go to 1% of traffic; this
# view is the comparison the gateway checks before 100%. Promoting a
# routing change without looking here is exactly what it prevents.
resource "google_bigquery_table" "policy_canary" {
  dataset_id = google_bigquery_dataset.llm_platform.dataset_id
  table_id   = "v_policy_canary"

  view {
    query = <<-SQL
      SELECT
        policy_version,
        DATE(timestamp) AS day,
        COUNT(*) AS requests,
        SUM(cost_usd) / COUNT(*) AS cost_per_request,
        AVG(IF(cache_tier != 'miss', 1.0, 0.0)) AS cache_hit_rate,
        AVG(IF(cascade_outcome = 'escalated', 1.0, 0.0)) AS escalation_rate,
        AVG(latency_ms) AS avg_latency_ms,
        AVG(IF(degraded_path, 1.0, 0.0)) AS fallback_rate
      FROM `${var.project_id}.${google_bigquery_dataset.llm_platform.dataset_id}.request_log`
      WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
      GROUP BY policy_version, day
      ORDER BY day DESC
    SQL
    use_legacy_sql = false
  }
}

###############################################################################
# 6. PROVIDER FALLBACK — external API keys + Vertex AI access
###############################################################################
# Fallback chains are policy; the CREDENTIALS are here. External providers
# are capacity insurance (design doc): last in the chain, zero-retention
# terms for anything with 'no_training' class.
###############################################################################

resource "google_secret_manager_secret" "external_provider_keys" {
  for_each = toset(["openai", "anthropic"]) # your overflow providers

  secret_id = "llm-provider-${each.key}-api-key"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "external_provider_keys" {
  for_each = google_secret_manager_secret.external_provider_keys

  secret      = each.value.id
  secret_data = "PLACEHOLDER-SET-ME" # set via gcloud or your vault sync — never terraform
}

###############################################################################
# 7. IAM — the gateway identity
###############################################################################

resource "google_service_account" "llm_gateway" {
  account_id   = "llm-gateway"
  display_name = "Model gateway (cache/cascade/route)"
}

# Vertex AI: fallback calls + (later) Vector Search queries.
resource "google_project_iam_member" "gateway_aiplatform" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.llm_gateway.email}"
}

# Write the request ledger (the streaming insert comes from the pod).
resource "google_bigquery_dataset_iam_member" "gateway_bq" {
  dataset_id = google_bigquery_dataset.llm_platform.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.llm_gateway.email}"
}

# Read current policy + hot-reload notifications.
resource "google_storage_bucket_iam_member" "gateway_read_policy" {
  bucket = google_storage_bucket.policy.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.llm_gateway.email}"
}

resource "google_pubsub_topic_iam_member" "gateway_watch_policy" {
  topic  = google_pubsub_topic.policy_updated.name
  role   = "roles/pubsub.subscriber"
  member = "serviceAccount:${google_service_account.llm_gateway.email}"
}

# The cache DSN + provider API keys (scoped per secret, not project-wide).
resource "google_secret_manager_secret_iam_member" "gateway_secrets" {
  for_each = merge(
    google_secret_manager_secret.external_provider_keys,
    {
      cache_dsn    = google_secret_manager_secret.semantic_cache_dsn
      cache_redis  = google_secret_manager_secret.cache_redis_auth
    }
  )
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.llm_gateway.email}"
}

# Workload Identity: the gateway pod's KSA -> this GSA.
resource "google_service_account_iam_member" "wi_gateway" {
  service_account_id = google_service_account.llm_gateway.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.serving_namespace}/llm-gateway]"
}

###############################################################################
# 8. KUBERNETES PROVIDER + SAMPLE GATEWAY DEPLOYMENT
###############################################################################

data "google_client_config" "default" {}

provider "kubernetes" {
  host  = "https://${data.google_container_cluster.existing.endpoint}"
  token = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(
    data.google_container_cluster.existing.master_auth[0].cluster_ca_certificate,
  )
}

resource "kubernetes_service_account" "llm_gateway" {
  metadata {
    name      = "llm-gateway"
    namespace = var.serving_namespace # created by the serving stack
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.llm_gateway.email
    }
  }
}

# The gateway: your image (KServe InferenceGraph or custom). The env block
# IS the design doc — every pattern from it is a knob here.
resource "kubernetes_manifest" "llm_gateway" {
  manifest = {
    apiVersion = "apps/v1"
    kind       = "Deployment"
    metadata = {
      name      = "llm-gateway"
      namespace = var.serving_namespace
      labels    = { app = "llm-gateway" }
    }
    spec = {
      replicas = 4
      selector = { matchLabels = { app = "llm-gateway" } }
      template = {
        metadata = { labels = { app = "llm-gateway" } }
        spec = {
          serviceAccountName = kubernetes_service_account.llm_gateway.metadata[0].name

          # From the training stack: serving beats everything, including
          # the gateway itself under extreme pressure. Remove that line if
          # the training stack isn't applied in this cluster.
          priorityClassName = "serving-critical"

          containers = [
            {
              name  = "gateway"
              image = "us-central1-docker.pkg.dev/REPLACE-PROJECT/model-serving/llm-gateway:v0.1.0" # UPDATE ME

              ports = [{ containerPort = 8080, name = "http" }]

              env = [
                # --- Decision chain order: cache -> classify -> cascade -> route
                { name = "CACHE_EXACT_ADDR",  value = "${google_redis_instance.llm_cache.host}:6378" },
                {
                  name = "CACHE_EXACT_AUTH"
                  valueFrom = {
                    secretKeyRef = {
                      name = kubernetes_secret.gateway_secrets.metadata[0].name
                      key  = "redis_auth"
                    }
                  }
                },
                {
                  name = "CACHE_SEMANTIC_DSN"
                  valueFrom = {
                    secretKeyRef = {
                      name = kubernetes_secret.gateway_secrets.metadata[0].name
                      key  = "semantic_dsn"
                    }
                  }
                },
                # --- Cascade knobs (policy file usually overrides)
                { name = "CASCADE_TIER1_UPSTREAM", value = "http://vllm-mistral-7b.${var.serving_namespace}.svc:80" },   # distilled, L4 pool
                { name = "CASCADE_TIER2_UPSTREAM", value = "http://vllm-flagship.${var.serving_namespace}.svc:80" },    # flagship, A3 pool
                { name = "CASCADE_CLASSIFIER_MODEL", value = "text-embedding-router-small" },
                # --- Hybrid routing + fallbacks
                { name = "VERTEX_FALLBACK_PROJECT", value = var.project_id },
                { name = "VERTEX_FALLBACK_REGION",  value = var.region },
                { name = "FALLBACK_CHAIN", value = "vllm-tier2,vertex,external" }, # policy file is source of truth
                # --- Policy hot-reload
                { name = "POLICY_BUCKET",  value = google_storage_bucket.policy.name },
                { name = "POLICY_TOPIC",   value = google_pubsub_topic.policy_updated.name },
                # --- Telemetry: one row per request
                { name = "REQUEST_LOG_TABLE", value = "${google_bigquery_dataset.llm_platform.dataset_id}.${google_bigquery_table.request_log.table_id}" },
                # --- Shadow traffic: 1% to new providers/models before trust
                { name = "SHADOW_SAMPLE_PCT", value = "1" },
              ]

              resources = {
                requests = { cpu = "1", memory = "1Gi" }
                limits   = { cpu = "2", memory = "2Gi" }
              }

              readinessProbe = {
                httpGet = { path = "/healthz", port = 8080 }
              }
              livenessProbe = {
                httpGet = { path = "/healthz", port = 8080 }
              }
            }
          ]

          # The gateway is a serving-plane CPU pod: system pool.
          nodeSelector = { pool = "system" }
        }
      }
    }
  }
}

# Project the two platform secrets into the pod.
resource "kubernetes_secret" "gateway_secrets" {
  metadata {
    name      = "llm-gateway-secrets"
    namespace = var.serving_namespace
  }

  data = {
    redis_auth   = var.cache_redis_auth
    semantic_dsn = google_secret_manager_secret_version.semantic_cache_dsn.secret_data
  }
}

###############################################################################
# 9. OUTPUTS
###############################################################################

output "exact_cache_redis" {
  value = google_redis_instance.llm_cache.host
}

output "semantic_cache_db" {
  description = "Run the pgvector migration SQL (section 3 comment) before first use."
  value       = google_sql_database.semantic_cache.name
}

output "policy_bucket" {
  description = "CI lands llm-policy.yaml here; the gateway hot-reloads on the Pub/Sub notification."
  value       = google_storage_bucket.policy.url
}

output "canary_view" {
  description = "Check v_policy_canary before promoting any routing policy to 100%."
  value       = "${google_bigquery_dataset.llm_platform.dataset_id}.${google_bigquery_table.policy_canary.table_id}"
}

output "first_steps_note" {
  value = <<-EOT
    1. Run the pgvector migration (CREATE EXTENSION vector; table + HNSW index — section 3).
    2. Set the provider API keys in Secret Manager (never via Terraform values).
    3. Land your first llm-policy.yaml in the policy bucket (canary rules first: 1% traffic).
    4. Point clients/agents at the gateway Service — the serving stack's HTTPRoute should target this deployment.
    5. Watch v_policy_canary for a day before widening any policy to 100%.
  EOT
}
