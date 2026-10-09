###############################################################################
# 0. TERRAFORM + PROVIDERS — the AGENTIC layer
###############################################################################
# This stack adds the agentic planes from the architecture doc on top of the
# serving infrastructure (cluster, GPU pools, KEDA, Argo Rollouts, Gateway).
# It assumes:
#   - The GKE regional cluster from the serving Terraform already exists.
#   - KEDA is installed (from the serving stack) — the ScaledObject/ScaledJob
#     CRDs must exist before `kubernetes_manifest` can plan.
#
# It creates:
#   - Two new node pools: agent-orchestrator (CPU) and tool-sandbox (gVisor)
#   - Memorystore Redis (session state)
#   - Cloud SQL Postgres with pgvector (durable checkpoints + long-term memory)
#   - Pub/Sub topic/subscription (async + batch agent tasks)
#   - Least-privilege SAs + Workload Identity bindings (orchestrator, tools, batch)
#   - Namespaces, default-deny NetworkPolicies for the tool sandbox
#   - Sample: session router + tool sandbox Deployment + KEDA scalers
#
# Prereqs: gcloud auth application-default login; Private Service Access
# peering (created in section 5) must not already exist for this VPC.
#
# Two-phase apply on a fresh namespace: infra first, then k8s objects:
#   terraform apply -target=google_container_node_pool.agent_orchestrator \
#                   -target=google_container_node_pool.tool_sandbox
#   terraform apply   # second pass plans the kubernetes_manifest resources
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
# 1. VARIABLES
###############################################################################

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

# The existing cluster created by the serving Terraform.
variable "cluster_name" {
  type    = string
  default = "model-serving"
}

# The VPC shared with the serving stack. Subnet peering is done on the VPC.
variable "vpc_name" {
  type    = string
  default = "model-serving-vpc"
}

variable "db_password" {
  description = "Cloud SQL agent user password. In prod: fetch from your vault or use Secret Manager rotation."
  type        = string
  default     = "CHANGE-ME"
  sensitive   = true
}

variable "redis_auth" {
  description = "AUTH token for Memorystore. Rotate via your secrets process."
  type        = string
  default     = "CHANGE-ME"
  sensitive   = true
}

locals {
  ns_core   = "agents"        # orchestrator + session router + memory nodes
  ns_tools  = "tool-sandbox"  # sandboxed tool execution — the security boundary

  workload_pool = "${var.project_id}.svc.id.goog"
}

# Look up the existing cluster + network instead of recreating them.
data "google_container_cluster" "existing" {
  name     = var.cluster_name
  location = var.region
}

data "google_compute_network" "vpc" {
  name = var.vpc_name
}

###############################################################################
# 2. NODE POOLS — the two new planes
###############################################################################

# --- Orchestrator plane: CPU-bound (agents CALL models, they don't run them).
# Cheap pods, so keep generous headroom — a slow CPU plane starves the GPUs.
resource "google_container_node_pool" "agent_orchestrator" {
  name     = "agent-orchestrator"
  cluster  = data.google_container_cluster.existing.name
  location = var.region

  autoscaling {
    min_node_count = 2
    max_node_count = 12
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type = "c3-standard-22"
    service_account = (
      # Reuse the node SA created in the serving stack if present;
      # otherwise GKE uses the default compute SA (tighten in prod!).
      data.google_container_cluster.existing.node_config[0].service_account
    )
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    # Keep tool pods and serving workloads OFF this pool.
    taint {
      key    = "workload"
      value  = "agent-orchestrator"
      effect = "NO_SCHEDULE"
    }

    labels = { pool = "agent-orchestrator" }
  }
}

# --- Tool sandbox plane: gVisor (userspace kernel) for agent tool execution.
# This is where prompt-injected code and web fetches run. No GPUs, no mercy.
resource "google_container_node_pool" "tool_sandbox" {
  name     = "tool-sandbox"
  cluster  = data.google_container_cluster.existing.name
  location = var.region

  autoscaling {
    min_node_count = 1
    max_node_count = 10
  }

  node_config {
    machine_type = "e2-standard-8"
    image_type  = "COS_CONTAINERD" # gVisor requires Container-Optimized OS

    # Enables gVisor on the pool; GKE then auto-creates the `gvisor`
    # RuntimeClass that pods opt into via runtimeClassName: gvisor.
    sandbox_config {
      sandbox_type = "gvisor"
    }

    taint {
      key    = "workload"
      value  = "tool-sandbox"
      effect = "NO_SCHEDULE"
    }

    labels = { pool = "tool-sandbox" }
  }
}

# Batch agent runs: REUSE the Spot pool from the serving stack (`llm-spot`)
# for GKE Jobs, or add a dedicated `agent-batch` Spot pool here if you want
# the cost centers separated. Checkpointed runs make Spot safe — preemption
# means resume, not re-run.

###############################################################################
# 3. MEMORISTORE REDIS — hot session state, streaming buffers, presence
###############################################################################

resource "google_redis_instance" "sessions" {
  name           = "agent-sessions"
  tier           = "STANDARD_HA"        # cross-zone replica; sessions are precious
  memory_size_gb = 5

  location_id         = "us-central1-a" # pin primary + replica zones explicitly
  alternative_location_id = "us-central1-f"

  authorized_network = data.google_compute_network.vpc.id
  connect_mode       = "PRIVATE_SERVICE_ACCESS" # private IP via PSA peering (section 5)

  transit_encryption_mode = "SERVER_AUTHENTICATION" # TLS always on
  auth_enabled            = true
  auth_string             = var.redis_auth

  # Redis 7.x for streams/observability niceties.
  redis_version = "REDIS_7_0"

  # Persistence: AOF every second — a full node restart should not lose
  # in-flight agent turns.
  persistence_config {
    mode = "AOF"
  }
}

###############################################################################
# 4. CLOUD SQL POSTGRES — durable checkpoints + long-term memory (pgvector)
###############################################################################

resource "google_sql_database_instance" "agent_state" {
  name             = "agent-state"
  database_version = "POSTGRES_16"

  settings {
    tier                = "db-custom-4-15360" # 4 vCPU / 15GB — right-size per run volume
    availability_type   = "REGIONAL"         # HA failover for the checkpoint store
    deletion_protection = true               # never destroy run state by accident

    # pgvector for long-term user memory + RAG-as-node patterns.
    database_flags {
      name  = "cloudsql.extensions"
      value = "pgvector"
    }

    ip_configuration {
      ipv4_enabled    = false # private IP only — reachable from pods via PSA
      private_network = data.google_compute_network.vpc.id
      require_ssl     = true
    }

    backup_configuration {
      enabled                    = true
      point_in_time_recovery_enabled = true
    }
  }
}

resource "google_sql_database" "checkpoints" {
  name     = "agent_state"
  instance = google_sql_database_instance.agent_state.name
}

resource "google_sql_user" "agent" {
  name     = "agent"
  instance = google_sql_database_instance.agent_state.name
  password = var.db_password
}

# The connection string the orchestrator uses. Stored in Secret Manager, then
# projected into pods (below). In prod consider the Secret Store CSI driver
# instead of a Terraform-managed k8s Secret.
resource "google_secret_manager_secret" "db_conn" {
  secret_id = "agent-db-conn"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "db_conn" {
  secret = google_secret_manager_secret.db_conn.id
  # ip_address[0] is the private IP the PSA peering hands out.
  secret_data = "postgresql://agent:${var.db_password}@${google_sql_database_instance.agent_state.ip_address[0].ip_address}:5432/agent_state?sslmode=require"
}

###############################################################################
# 5. PRIVATE SERVICE ACCESS — the peering that makes Redis + SQL reachable
###############################################################################

# One dedicated range for Google-managed services (Memorystore, Cloud SQL).
resource "google_compute_global_address" "psa_range" {
  name          = "google-managed-services-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  network       = data.google_compute_network.vpc.id
  address       = "10.20.0.0"
  prefix_length = 16
}

resource "google_service_networking_connection" "psa" {
  network                 = data.google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.psa_range.name]
}

###############################################################################
# 6. PUB/SUB — async agent tasks + batch backlog
###############################################################################

resource "google_pubsub_topic" "agent_tasks" {
  name = "agent-tasks"

  # BigQuery subscription downstream: the per-step run logs (cost per task!)
  message_retention_duration = "604800s" # 7 days
}

# Pull subscription for batch agent runs (KEDA ScaledJob scales on its backlog).
resource "google_pubsub_subscription" "batch_tasks" {
  name  = "agent-batch-tasks"
  topic = google_pubsub_topic.agent_tasks.name

  ack_deadline_seconds = 600 # agent steps are slow; long ack deadline = fewer redelivers

  retention_duration = "604800s" # 7d

  expiration_policy {
    # Never expire — an idle subscription that vanishes silently breaks KEDA.
    ttl = ""
  }

  # Retry with backoff so a crashing tool pod doesn't hot-loop.
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

###############################################################################
# 7. ARTIFACT BUCKET + SERVICE ACCOUNTS (least privilege per plane)
###############################################################################

resource "google_storage_bucket" "agent_artifacts" {
  name          = "agent-artifacts-${var.project_id}"
  location      = var.region
  # Files agents produce (code, docs, exports). Per-tenant prefixes + IAM
  # in a real multi-tenant setup.
  uniform_bucket_level_access = true
}

# --- Orchestrator SA: state stores, pub/sub, artifacts, tracing.
resource "google_service_account" "orchestrator" {
  account_id   = "agent-orchestrator"
  display_name = "Agent orchestrator + session router pods"
}

# --- Tool SA: deliberately near-zero permissions. Tools reach the world via
# the egress allowlist proxy, NOT via cloud credentials. If a prompt injection
# takes over a tool pod, there is (almost) nothing here to steal.
resource "google_service_account" "tool_executor" {
  account_id   = "agent-tool-executor"
  display_name = "Sandboxed tool execution pods"
}

# --- Batch runner SA: for Spot-based batch agent jobs.
resource "google_service_account" "batch_runner" {
  account_id   = "agent-batch-runner"
  display_name = "Batch agent runs (GKE Jobs on Spot)"
}

# Orchestrator grants (scoped to resources, not project-wide where possible):
resource "google_storage_bucket_iam_member" "orch_readwrite_artifacts" {
  bucket = google_storage_bucket.agent_artifacts.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_pubsub_topic_iam_member" "orch_publish" {
  topic  = google_pubsub_topic.agent_tasks.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_pubsub_subscription_iam_member" "batch_consume" {
  subscription = google_pubsub_subscription.batch_tasks.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.batch_runner.email}"
}

resource "google_secret_manager_secret_iam_member" "orch_db_conn" {
  secret_id = google_secret_manager_secret.db_conn.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.orchestrator.email}"
}

# Project-wide roles the orchestrator genuinely needs (keep this list short):
resource "google_project_iam_member" "orch_project_roles" {
  for_each = toset([
    "roles/cloudtrace.agent",        # OpenTelemetry -> Cloud Trace
    "roles/monitoring.metricWriter", # OTel -> Cloud Monitoring
    "roles/logging.logWriter",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.orchestrator.email}"
}

# Workload Identity bindings: namespace/KSA -> GSA. No keys anywhere.
resource "google_service_account_iam_member" "wi_orchestrator" {
  service_account_id = google_service_account.orchestrator.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.workload_pool}[${local.ns_core}/agent-orchestrator]"
}

resource "google_service_account_iam_member" "wi_tools" {
  service_account_id = google_service_account.tool_executor.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.workload_pool}[${local.ns_tools}/agent-tool-executor]"
}

resource "google_service_account_iam_member" "wi_batch" {
  service_account_id = google_service_account.batch_runner.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.workload_pool}[${local.ns_core}/agent-batch-runner]"
}

###############################################################################
# 8. KUBERNETES PROVIDER WIRING (self-contained for this stack)
###############################################################################

data "google_client_config" "default" {}

provider "kubernetes" {
  host = "https://${data.google_container_cluster.existing.endpoint}"
  token = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(
    data.google_container_cluster.existing.master_auth[0].cluster_ca_certificate,
  )
}

###############################################################################
# 9. NAMESPACES + KSAs
###############################################################################

resource "kubernetes_namespace" "agents" {
  metadata {
    name = local.ns_core
  }
}

resource "kubernetes_namespace" "tool_sandbox" {
  metadata {
    name = local.ns_tools

    # Enforce hardened settings on every pod in the sandbox namespace.
    labels = {
      "pod-security.kubernetes.io/enforce" = "restricted"
    }
  }
}

resource "kubernetes_service_account" "agent_orchestrator" {
  metadata {
    name      = "agent-orchestrator"
    namespace = kubernetes_namespace.agents.metadata[0].name
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.orchestrator.email
    }
  }
  automount_service_account_token = false # pods get their identity via WI, not tokens
}

resource "kubernetes_service_account" "tool_executor" {
  metadata {
    name      = "agent-tool-executor"
    namespace = kubernetes_namespace.tool_sandbox.metadata[0].name
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.tool_executor.email
    }
  }
  automount_service_account_token = false
}

resource "kubernetes_service_account" "batch_runner" {
  metadata {
    name      = "agent-batch-runner"
    namespace = kubernetes_namespace.agents.metadata[0].name
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.batch_runner.email
    }
  }
}

# The DB connection string as a k8s Secret, sourced from Secret Manager.
data "google_secret_manager_secret_version" "db_conn" {
  secret  = google_secret_manager_secret.db_conn.secret_id
  version = google_secret_manager_secret_version.db_conn.version
}

resource "kubernetes_secret" "db_conn" {
  metadata {
    name      = "agent-db-conn"
    namespace = kubernetes_namespace.agents.metadata[0].name
  }
  data = {
    connection_string = data.google_secret_manager_secret_version.db_conn.secret_data
  }
}

###############################################################################
# 10. NETWORK POLICIES — default-deny in the tool sandbox
###############################################################################
# Threat model: a tool pod is assumed compromised (prompt injection).
# Ingress: only from the agents namespace. Egress: only DNS + the allowlist
# proxy. Everything else dropped. Tighten further with per-tool policies.
###############################################################################

resource "kubernetes_manifest" "tools_default_deny" {
  manifest = {
    apiVersion = "networking.k8s.io/v1"
    kind       = "NetworkPolicy"
    metadata = {
      name      = "default-deny-all"
      namespace = kubernetes_namespace.tool_sandbox.metadata[0].name
    }
    spec = {
      podSelector = {} # all pods in the namespace
      policyTypes = ["Ingress", "Egress"]
      # Empty ingress/egress lists = deny all. Allow rules come next.
      ingress = []
      egress  = []
    }
  }
}

# Allow DNS — without this, even allowed egress can't resolve names.
resource "kubernetes_manifest" "tools_allow_dns" {
  manifest = {
    apiVersion = "networking.k8s.io/v1"
    kind       = "NetworkPolicy"
    metadata = {
      name      = "allow-dns"
      namespace = kubernetes_namespace.tool_sandbox.metadata[0].name
    }
    spec = {
      podSelector = {}
      policyTypes = ["Egress"]
      egress = [
        {
          to = [{ namespaceSelector = { matchLabels = { "kubernetes.io/metadata.name" = "kube-system" } } }]
          ports = [
            { protocol = "UDP", port = 53 },
            { protocol = "TCP", port = 53 },
          ]
        }
      ]
    }
  }
}

# Allow ingress ONLY from the orchestrator namespace.
resource "kubernetes_manifest" "tools_allow_from_agents" {
  manifest = {
    apiVersion = "networking.k8s.io/v1"
    kind       = "NetworkPolicy"
    metadata = {
      name      = "allow-from-agents"
      namespace = kubernetes_namespace.tool_sandbox.metadata[0].name
    }
    spec = {
      podSelector = {}
      policyTypes = ["Ingress"]
      ingress = [{
        from = [{
          namespaceSelector = { matchLabels = { "kubernetes.io/metadata.name" = local.ns_core } }
        }]
      }]
    }
  }
}

# EGRESS ALLOWLIST PROXY: the intended end state is an explicit-allow egress
# path (a small proxy deployment in the namespace + a NetworkPolicy allowing
# egress only to it), with per-domain rules on the proxy. Deploy that proxy
# and add `allow-to-proxy` here; the deny-by-default baseline is what keeps
# you safe until you do.

###############################################################################
# 11. SAMPLE WORKLOADS
###############################################################################

# --- 11a. Session router (CPU, cheap, scales on active SSE streams).
# Production image: your own; this is the shape — env points at Redis and OTel.
resource "kubernetes_manifest" "session_router" {
  manifest = {
    apiVersion = "apps/v1"
    kind       = "Deployment"
    metadata = {
      name      = "session-router"
      namespace = kubernetes_namespace.agents.metadata[0].name
      labels    = { app = "session-router" }
    }
    spec = {
      replicas = 3
      selector = { matchLabels = { app = "session-router" } }
      template = {
        metadata = { labels = { app = "session-router" } }
        spec = {
          serviceAccountName = kubernetes_service_account.agent_orchestrator.metadata[0].name
          containers = [
            {
              name  = "router"
              image = "ghcr.io/yourorg/agent-session-router:v0.1.0" # UPDATE ME
              ports = [{ containerPort = 8080, name = "http" }]
              env = [
                {
                  name  = "REDIS_ADDR"
                  value = "${google_redis_instance.sessions.host}:6379"
                },
                {
                  name = "OTEL_EXPORTER_OTLP_ENDPOINT"
                  # Collector in-cluster; or point straight at Cloud Trace's
                  # OTLP endpoint with the WI identity from section 7.
                  value = "http://otel-collector.observability.svc:4318"
                },
              ]
              resources = {
                requests = { cpu = "500m", memory = "512Mi" }
                limits   = { cpu = "1", memory = "1Gi" }
              }
              readinessProbe = {
                httpGet = { path = "/healthz", port = 8080 }
              }
              livenessProbe = {
                httpGet = { path = "/healthz", port = 8080 }
              }
            }
          ]
          nodeSelector = { pool = "agent-orchestrator" }
          tolerations = [
            {
              key      = "workload"
              operator = "Equal"
              value    = "agent-orchestrator"
              effect   = "NoSchedule"
            }
          ]
        }
      }
    }
  }
}

# --- 11b. Tool sandbox worker: gVisor runtime, no cloud creds, sandboxed.
resource "kubernetes_manifest" "tool_worker" {
  manifest = {
    apiVersion = "apps/v1"
    kind       = "Deployment"
    metadata = {
      name      = "tool-worker"
      namespace = kubernetes_namespace.tool_sandbox.metadata[0].name
      labels    = { app = "tool-worker" }
    }
    spec = {
      replicas = 2
      selector = { matchLabels = { app = "tool-worker" } }
      template = {
        metadata = { labels = { app = "tool-worker" } }
        spec = {
          serviceAccountName = kubernetes_service_account.tool_executor.metadata[0].name
          # THE key line: the userspace-kernel sandbox. Combined with the
          # default-deny policies above, this pod is a jail.
          runtimeClassName = "gvisor"
          containers = [
            {
              name  = "tool"
              image = "ghcr.io/yourorg/agent-tools:v0.1.0" # UPDATE ME
              env = [
                {
                  name  = "TOOLSET"
                  value = "web_fetch,code_exec,search"
                },
              ]
              # Hard resource ceilings — tool code must not be able to
              # exhaust the node.
              resources = {
                requests = { cpu = "500m", memory = "512Mi" }
                limits   = { cpu = "2", memory = "2Gi" }
              }
              securityContext = {
                allowPrivilegeEscalation = false
                readOnlyRootFilesystem  = true
                runAsNonRoot             = true
                capabilities             = { drop = ["ALL"] }
              }
            }
          ]
          nodeSelector = { pool = "tool-sandbox" }
          tolerations = [
            {
              key      = "workload"
              operator = "Equal"
              value    = "tool-sandbox"
              effect   = "NoSchedule"
            }
          ]
        }
      }
    }
  }
}

# --- 11c. KEDA ScaledObject: orchestrator/router scale on PENDING STEPS,
# not CPU. The router exports `agent:active_runs` and `agent:pending_steps`
# via /metrics; scale on what actually predicts load.
resource "kubernetes_manifest" "router_scaled_object" {
  manifest = {
    apiVersion = "keda.sh/v1alpha1"
    kind       = "ScaledObject"
    metadata = {
      name      = "session-router"
      namespace = kubernetes_namespace.agents.metadata[0].name
    }
    spec = {
      scaleTargetRef  = { name = "session-router" }
      minReplicaCount = 3
      maxReplicaCount = 20
      pollingInterval = 10
      cooldownPeriod  = 120
      triggers = [
        {
          type = "prometheus"
          metadata = {
            serverAddress = "http://session-router.${local.ns_core}.svc:8080/metrics"
            query         = "sum(agent:active_sse_streams)"
            threshold     = "200" # add a replica per ~200 active streams
          }
        },
      ]
    }
  }
}

# --- 11d. KEDA ScaledJob: batch agent runs scale with the Pub/Sub backlog.
# Each message = one long-running agent run (research, bulk enrichment),
# executed on Spot in the llm-spot pool, checkpointed to Postgres/GCS.
resource "kubernetes_manifest" "agent_batch_scaledjob" {
  manifest = {
    apiVersion = "keda.sh/v1alpha1"
    kind       = "ScaledJob"
    metadata = {
      name      = "agent-batch"
      namespace = kubernetes_namespace.agents.metadata[0].name
    }
    spec = {
      maxReplicaCount = 10 # cap = cap on concurrent Spot spend
      pollingInterval = 30
      jobTargetRef = {
        backoffLimit     = 2
        completions      = 1
        completionMode   = "Indexed"
        ttlSecondsAfterFinished = 3600 # cleanup finished Job pods
        template = {
          spec = {
            restartPolicy = "Never" # Jobs restart whole, never in-place
            serviceAccountName = kubernetes_service_account.batch_runner.metadata[0].name
            containers = [
              {
                name  = "agent-run"
                image = "ghcr.io/yourorg/agent-runner:v0.1.0" # UPDATE ME
                env = [
                  {
                    name  = "DB_CONN"
                    valueFrom = {
                      secretKeyRef = {
                        name = kubernetes_secret.db_conn.metadata[0].name
                        key  = "connection_string"
                      }
                    }
                  },
                  {
                    name  = "REDIS_ADDR"
                    value = "${google_redis_instance.sessions.host}:6379"
                  },
                ]
                resources = {
                  requests = { cpu = "2", memory = "4Gi" }
                  limits   = { cpu = "4", memory = "8Gi" }
                }
              }
            ]
            # Batch runs reuse the Spot GPU pool from the serving stack:
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
            # KEDA's operator itself needs Pub/Sub read access: give the
            # KEDA operator a WI-bound SA with roles/pubsub.viewer on this
            # subscription, or fall back to a credentials Secret.
            subscriptionName = google_pubsub_subscription.batch_tasks.id
            mode             = "SubscriptionSize"
            value            = "1" # one Job per backlog message
          }
        },
      ]
    }
  }
}

###############################################################################
# 12. OUTPUTS
###############################################################################

output "redis_host" {
  description = "Memorystore endpoint for the session router / orchestrator env."
  value       = google_redis_instance.sessions.host
}

output "postgres_private_ip" {
  value = google_sql_database_instance.agent_state.ip_address[0].ip_address
}

output "pubsub_topic" {
  value = google_pubsub_topic.agent_tasks.id
}

output "artifact_bucket" {
  value = google_storage_bucket.agent_artifacts.url
}

output "next_steps_note" {
  description = "Manual follow-ups after apply."
  value       = <<-EOT
    1. Point your SSE Gateway/HTTPRoute at the session-router Service (Gateway from the serving stack).
    2. Create the session-router/agent Service objects (omitted here — standard ClusterIP on :8080).
    3. Grant the KEDA operator's SA roles/pubsub.viewer on the batch subscription (or switch to a credentials Secret).
    4. Deploy the egress allowlist proxy in tool-sandbox and open exactly one egress flow to it.
  EOT
}
