###############################################################################
# 0. TERRAFORM + PROVIDERS
###############################################################################
# In a real repo you'd split this into providers.tf / variables.tf / network.tf /
# cluster.tf / iam.tf / k8s.tf. Single file here for readability.
#
# Prerequisites before `terraform apply`:
#   gcloud auth application-default login
#   Enable APIs: compute, container, artifactregistry, storage, iam, cloudkms,
#               certificatemanager (if you terminate TLS at the Gateway)
#
# Apply in two passes on a brand-new cluster (see the note on
# kubernetes_manifest below):
#   terraform apply -target=google_container_cluster.primary
#   ... then a full `terraform apply` for the k8s layer.
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
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.15"
    }
  }

  # Plug in your backend — keep state remote + locked, never local for prod.
  # backend "gcs" {
  #   bucket = "your-tf-state-bucket"
  #   prefix = "gke-model-serving"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

###############################################################################
# 1. VARIABLES
###############################################################################

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Region for the cluster (e.g. europe-west1, us-central1). Pick one that has A3/H100 capacity."
  type        = string
  default     = "us-central1"
}

variable "cluster_name" {
  type    = string
  default = "model-serving"
}

variable "node_locations" {
  description = "Zones the regional cluster spans. Three zones = zonal-failure tolerance."
  type        = list(string)
  default     = ["us-central1-a", "us-central1-b", "us-central1-c"]
}

variable "model_bucket_name" {
  description = "GCS bucket holding model weights (the 'poor man's model registry')."
  type        = string
  default     = "model-registry-weights"
}

locals {
  # The GSA name and the namespace/KSA used by serving pods. Wire Identity
  # requires the exact namespace/KSA string below to match the Deployment.
  serving_namespace  = "model-serving"
  serving_ksa        = "model-serving"
  workload_pool      = "${var.project_id}.svc.id.goog"
  model_bucket_fqdn  = "${var.model_bucket_name}-${var.project_id}"
}

###############################################################################
# 2. NETWORK — VPC, private subnet, Cloud NAT
###############################################################################

resource "google_compute_network" "vpc" {
  name                    = "model-serving-vpc"
  auto_create_subnetworks = false # explicit subnets only — no surprise /16s
}

resource "google_compute_subnetwork" "cluster" {
  name          = "${var.cluster_name}-subnet"
  region        = var.region
  network       = google_compute_network.vpc.id
  ip_cidr_range = "10.10.0.0/20"

  # Secondary ranges for VPC-native (alias IP) GKE.
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.11.0.0/16" # ~65k pod IPs — size for max node count
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.12.0.0/20"
  }

  # Pods can reach Google APIs (GCS, Artifact Registry) without external IPs.
  private_ip_google_access = true
}

# Cloud NAT lets private nodes pull images/weights from the internet
# (e.g. Docker Hub) when needed. Private Google Access covers Google-hosted stuff.
resource "google_compute_router" "router" {
  name    = "${var.cluster_name}-router"
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "${var.cluster_name}-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  source_subnet_ranges_to_nat        = ["ALL_SUBNETS_ALL_IP_RANGES"]
  nat_ip_allocate_option             = "AUTO_ONLY"

  # Keep NAT logs — costs and egress anomalies show up here first.
  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

###############################################################################
# 3. CMEK — KMS key for the model bucket
###############################################################################

resource "google_kms_key_ring" "models" {
  name     = "model-serving"
  location = var.region
}

resource "google_kms_crypto_key" "model_bucket" {
  name            = "model-weights"
  key_ring        = google_kms_key_ring.models.id
  purpose         = "ENCRYPT_DECRYPT"
  rotation_period = "7776000s" # 90-day rotation

  # Don't let a deleted key take your model bucket hostage.
  lifecycle {
    prevent_destroy = true
  }
}

# Every GCS bucket has a per-project service account that does the actual
# crypto. Grant it access to the key or the bucket 403s on first object write.
data "google_storage_project_service_account" "gpa" {}

resource "google_kms_crypto_key_iam_member" "gcs_uses_key" {
  crypto_key_id = google_kms_crypto_key.model_bucket.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${data.google_storage_project_service_account.gpa.email_address}"
}

###############################################################################
# 4. ARTIFACT REGISTRY + MODEL STORAGE
###############################################################################

resource "google_artifact_registry_repository" "serving_images" {
  location      = var.region
  repository_id = "model-serving"
  description   = "Serving images (vLLM/Triton wrappers, router, etc.)"
  format        = "DOCKER"

  docker_config {
    immutable_tags = false # Binary Authorization uses digest pinning anyway
  }
}

resource "google_storage_bucket" "model_weights" {
  name                        = local.model_bucket_fqdn
  location                    = var.region # REGIONAL: low latency from same-region nodes
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true # UBBLA is the only sane mode in 2026

  # CMEK from section 3.
  encryption {
    default_kms_key_name = google_kms_crypto_key.model_bucket.id
  }

  # Old versions of weights are your instant-rollback story. Never overwrite.
  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      with_state = "ARCHIVED" # or age-based if you prune aggressively
    }
    action {
      type = "Delete"
    }
  }

  # Don't let a `terraform destroy` vaporize the model weights.
  lifecycle {
    prevent_destroy = true
  }
}

###############################################################################
# 5. SERVICE ACCOUNTS (least privilege)
###############################################################################

# --- Node-level SA: what the kubelet/runtime needs. Never use the default
# compute SA in prod — it is over-privileged by default.
resource "google_service_account" "gke_nodes" {
  account_id   = "${var.cluster_name}-nodes"
  display_name = "GKE nodes for ${var.cluster_name}"
}

resource "google_project_iam_member" "gke_nodes_roles" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/autoscaling.metricsWriter", # for CA/autoscaler signals
    "roles/artifactregistry.reader",    # pull serving images
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

# --- Workload SA: what the *serving pods* need. Scoped to the bucket, not
# project-wide storage.viewer.
resource "google_service_account" "model_serving" {
  account_id   = "model-serving"
  display_name = "Model serving pods"
}

resource "google_storage_bucket_iam_member" "serving_can_read_weights" {
  bucket = google_storage_bucket.model_weights.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.model_serving.email}"
}

# Workload Identity Federation binding: namespace/KSA -> GSA.
# The pods then run with serviceAccountName = model-serving and no keys anywhere.
resource "google_service_account_iam_member" "workload_identity_binding" {
  service_account_id = google_service_account.model_serving.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.workload_pool}[${local.serving_namespace}/${local.serving_ksa}]"
}

###############################################################################
# 6. GKE CLUSTER — regional, private, Gateway API enabled
###############################################################################

resource "google_container_cluster" "primary" {
  name     = var.cluster_name
  location = var.region             # region => regional control plane across 3 zones
  node_locations = var.node_locations

  # We define our own pools below; delete the default.
  remove_default_node_pool = true
  initial_node_count       = 1

  network    = google_compute_network.vpc.id
  subnetwork = google_compute_subnetwork.cluster.id

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  # --- Private control plane & nodes ---
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false # keep a public endpoint for CI; lock with authorized_networks
    master_ipv4_cidr_block = "172.16.0.0/28"

    # Bastion/CI ranges that may reach the control plane. UPDATE ME.
    master_global_access_config {
      enabled = true
    }
  }

  # --- Modern cluster settings ---
  release_channel {
    channel = "REGULAR" # auto-patched, N-1 track; REGULAR is the boring, safe choice
  }

  gateway_api {
    channel = "CHANNEL_STANDARD" # enables Gateway API CRDs — the LB story on GKE
  }

  # GKE-managed DNS + Workload Identity are non-negotiables.
  dns_config {
    cluster_dns_provider = "CLOUD_DNS"
  }

  workload_identity_config {
    workload_pool = local.workload_pool
  }

  # Image streaming: pulls container layers lazily => much faster node
  # provisioning, which matters enormously when autoscaling multi-GB model images.
  gcfs_config {
    image_streaming_provider = "READ_ONLY"
  }

  # Only signed images run (paired with an upstream Binary Authorization policy).
  enable_binary_authorization = true

  # Long graceful-shutdown window so vLLM pods can finish in-flight generations
  # during upgrades; set matching terminationGracePeriodSeconds on the pods.
  node_config {
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  deletion_protection = true # prevents `terraform destroy` foot-guns on prod

  # Upgrade nodes gradually — a3 pool upgrades with a 16-GPU node take a while.
  node_pool_auto_config {
    network_tags = ["model-serving-node"]
  }

  depends_on = [
    google_compute_network.vpc,
    google_compute_subnetwork.cluster,
  ]
}

# Cluster-autoscaler resource limits cap the spend: CA may not exceed these
# per-machine-type counts no matter what the workloads request.
# NOTE: A3/H100 is frequently unavailable on-demand. For the a3 pool, consider
# GKE's Dynamic Workload Scheduler (flexible-start) via the google-beta
# provider's `autoscaling { dynamic_workers {...} }` block, or provision
# ahead of known peaks and keep burst on-demand.

###############################################################################
# 7. NODE POOLS — system, L4 online, A3 flagship, Spot batch
###############################################################################

# --- 7a. System pool: router, KEDA controller, Argo Rollouts, observability
# agents. Never share GPUs with these.
resource "google_container_node_pool" "system" {
  name       = "system"
  cluster    = google_container_cluster.primary.name
  location   = var.region

  autoscaling {
    min_node_count = 2
    max_node_count = 6
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type    = "e2-standard-8"
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    labels          = { pool = "system" }
  }
}

# --- 7b. L4 pool: workhorse for smaller models / distilled variants.
# g2-standard-24 pairs 1x L4 with 24 vCPU / 96GB — good cost-per-token for 7-13B.
resource "google_container_node_pool" "llm_l4" {
  name     = "llm-l4"
  cluster  = google_container_cluster.primary.name
  location = var.region

  autoscaling {
    min_node_count = 0 # scale-to-zero is fine for non-flagship models
    max_node_count = 20
    location_policy = "BALANCED"
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type    = "g2-standard-24"
    service_account = google_service_account.gke_nodes.email

    # GKE auto-installs the NVIDIA driver when the pool has accelerators.
    guest_accelerator {
      type  = "nvidia-l4"
      count = 1
      # For multiple small models per GPU, add gpu_sharing_config { gpu_sharing_strategy = "TIME_SHARING", max_shared_clients_per_gpu = 4 }
    }

    # NVMe scratch on the node: fast pod scheduling and image-streaming cache.
    ephemeral_storage_local_ssd_config {
      count = 1
    }

    # Keep CPU workloads off the GPUs and GPU pods from landing on system nodes.
    taint {
      key    = "nvidia.com/gpu"
      value  = "true"
      effect = "NO_SCHEDULE"
    }

    labels = { pool = "llm-l4" }
  }
}

# --- 7c. A3 / H100 pool: flagship 70B+ serving. 8x H100 80GB per node.
resource "google_container_node_pool" "llm_a3" {
  name     = "llm-a3"
  cluster  = google_container_cluster.primary.name
  location = var.region

  autoscaling {
    min_node_count = 0 # committed-use floor belongs in another pool if you need one
    max_node_count = 8 # ~8 nodes x 8 H100 = 64 GPUs max burst
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  # A3 upgrades are slow and disruptive; spread them.
  upgrade_settings {
    strategy        = "SURGE"
    max_surge       = 1
    max_unavailable = 0
  }

  node_config {
    machine_type    = "a3-highgpu-8g"
    service_account = google_service_account.gke_nodes.email

    guest_accelerator {
      type  = "nvidia-h100-80gb"
      count = 8
    }

    ephemeral_storage_local_ssd_config {
      count = 16 # fast scratch for multi-GB weight loads
    }

    taint {
      key    = "nvidia.com/gpu"
      value  = "true"
      effect = "NO_SCHEDULE"
    }

    labels = { pool = "llm-a3" }

    # A3 machines are not cheap — tag for cost dashboards.
    resource_labels = { workload = "llm-flagship" }
  }
}

# --- 7d. Spot pool: batch/offline inference only. Nodes vanish without
# warning, so only run restartable/checkpointed jobs here.
resource "google_container_node_pool" "llm_spot" {
  name     = "llm-spot"
  cluster  = google_container_cluster.primary.name
  location = var.region

  autoscaling {
    min_node_count = 0
    max_node_count = 30
  }

  node_config {
    machine_type    = "g2-standard-24"
    spot            = true # ~60-70% off; no SLA, no guaranteed availability
    service_account = google_service_account.gke_nodes.email

    guest_accelerator {
      type  = "nvidia-l4"
      count = 1
    }

    taint {
      key    = "spot"
      value  = "true"
      effect = "NO_SCHEDULE"
    }

    labels = { pool = "llm-spot" }
  }
}

###############################################################################
# 8. KUBERNETES / HELM PROVIDER WIRING
###############################################################################

# Authenticate the k8s/helm providers using the caller's gcloud credentials.
# In CI, use a GSA with container.developer + a token step instead.
data "google_client_config" "default" {}

data "google_container_cluster" "primary" {
  name     = google_container_cluster.primary.name
  location = var.region
}

provider "kubernetes" {
  host  = "https://${data.google_container_cluster.primary.endpoint}"
  token = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(
    data.google_container_cluster.primary.master_auth[0].cluster_ca_certificate,
  )
}

provider "helm" {
  kubernetes {
    host  = "https://${data.google_container_cluster.primary.endpoint}"
    token = data.google_client_config.default.access_token
    cluster_ca_certificate = base64decode(
      data.google_container_cluster.primary.master_auth[0].cluster_ca_certificate,
    )
  }
}

###############################################################################
# 9. IN-CLUSTER TOOLING (Helm)
###############################################################################

# --- KEDA: pod autoscaling on queue depth / concurrency rather than CPU.
resource "kubernetes_namespace" "serving" {
  metadata {
    name = local.serving_namespace
  }
}

resource "helm_release" "keda" {
  name      = "keda"
  namespace = "keda"
  # Pin the chart version in prod; `latest` is fine while prototyping.
  repository = "https://kedacore.github.io/charts"
  chart      = "keda"
  version    = "3.6.0"

  depends_on = [google_container_node_pool.system]
}

# --- Argo Rollouts: canary model deployments with SLO-based traffic shifting.
# (KServe is a valid alternative for simple model->endpoint mapping: install
# via its raw manifests; the vLLM Deployment below works identically under it.)
resource "helm_release" "argo_rollouts" {
  name       = "argo-rollouts"
  namespace  = "argo-rollouts"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argo-rollouts"
  version    = "2.38.0"

  depends_on = [google_container_node_pool.system]
}

###############################################################################
# 10. WORKLOAD IDENTITY — the KSA that serving pods run as
###############################################################################

resource "kubernetes_service_account" "model_serving" {
  metadata {
    name      = local.serving_ksa
    namespace = kubernetes_namespace.serving.metadata[0].name

    # This annotation is what binds KSA -> GSA via Workload Identity.
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.model_serving.email
    }
  }
}

###############################################################################
# 11. SAMPLE vLLM DEPLOYMENT + KEDA SCALED OBJECT + GATEWAY API
#
# NOTE: kubernetes_manifest validates against the live CRD schema at plan
# time. On a fresh cluster, apply sections 2-9 first
# (`terraform apply -target=...` or just run apply twice) so the KEDA CRDs
# exist before the ScaledObject is planned.
###############################################################################

# --- The vLLM Deployment. The pattern you'd repeat per model.
resource "kubernetes_manifest" "vllm_deployment" {
  manifest = {
    apiVersion = "apps/v1"
    kind       = "Deployment"
    metadata = {
      name      = "vllm-mistral-7b"
      namespace = kubernetes_namespace.serving.metadata[0].name
      labels    = { app = "vllm-mistral-7b" }
    }
    spec = {
      replicas = 2
      selector = { matchLabels = { app = "vllm-mistral-7b" } }
      template = {
        metadata = { labels = { app = "vllm-mistral-7b" } }
        spec = {
          serviceAccountName = kubernetes_service_account.model_serving.metadata[0].name

          # Let in-flight generations drain before SIGKILL during scale-downs.
          terminationGracePeriodSeconds = 120

          # Pin (and digest-pin, paired with Binary Authorization) in prod.
          containers = [
            {
              name  = "vllm"
              image = "vllm/vllm-openai:v0.9.2"

              # Weights straight from GCS — vLLM reads gs:// via its GCS support;
              # the pod's WI identity is what authorizes it. Point the path at
              # your model bucket. First load per node is slow; LocalSSD image
              # streaming + node caching mitigate repeats.
              args = [
                "--model=gs://${google_storage_bucket.model_weights.name}/mistral-7b-instruct",
                "--served-model-name=mistral-7b",
                # Continuous batching + PagedAttention: the throughput core.
                "--max-num-seqs=64",
                "--gpu-memory-utilization=0.92",
                "--port=8000",
              ]

              ports = [
                { name = "http", containerPort = 8000 }
              ]

              resources = {
                requests = {
                  cpu                = "4"
                  memory             = "24Gi"
                  "nvidia.com/gpu"   = "1"
                }
                limits = {
                  cpu                = "8"
                  memory             = "32Gi"
                  "nvidia.com/gpu"   = "1"
                }
              }

              # Startup probe covers the multi-GB weight load (cold start!).
              startupProbe = {
                httpGet = { path = "/health", port = 8000 }
                periodSeconds    = 10
                failureThreshold = 90 # allow up to 15 min for huge models
              }
              readinessProbe = {
                httpGet = { path = "/health", port = 8000 }
                periodSeconds = 5
              }
              livenessProbe = {
                httpGet = { path = "/health", port = 8000 }
                periodSeconds    = 10
                failureThreshold = 6
              }
            }
          ]

          # Land on the L4 pool; tolerate the GPU taint.
          nodeSelector = { "pool" = "llm-l4" }
          tolerations = [
            {
              key      = "nvidia.com/gpu"
              operator = "Equal"
              value    = "true"
              effect   = "NoSchedule"
            }
          ]
        }
      }
    }
  }
}

# --- ClusterIP Service fronting the Deployment. The HTTPRoute below targets it.
resource "kubernetes_service" "vllm" {
  metadata {
    name      = "vllm-mistral-7b"
    namespace = kubernetes_namespace.serving.metadata[0].name
  }
  spec = {
    selector = { app = "vllm-mistral-7b" }
    port {
      name        = "http"
      port        = 80
      targetPort  = 8000
    }
  }
}

# --- KEDA ScaledObject: scale on *queue depth*, not CPU.
# vLLM exposes Prometheus metrics on /metrics; the scaling question is
# "are requests waiting for a GPU slot?", and only vLLM knows the answer.
resource "kubernetes_manifest" "vllm_scaled_object" {
  manifest = {
    apiVersion = "keda.sh/v1alpha1"
    kind       = "ScaledObject"
    metadata = {
      name      = "vllm-mistral-7b"
      namespace = kubernetes_namespace.serving.metadata[0].name
    }
    spec = {
      scaleTargetRef = { name = "vllm-mistral-7b" }
      minReplicaCount = 2
      maxReplicaCount = 20
      pollingInterval = 10
      cooldownPeriod  = 300 # GPUs are expensive: don't flap the pool
      triggers = [
        {
          type = "prometheus"
          metadata = {
            # Scrape the Service so all replicas' metrics aggregate.
            serverAddress = "http://vllm-mistral-7b.${local.serving_namespace}.svc:80/metrics"
            query          = "sum(vllm:num_requests_waiting)"
            threshold      = "5" # add a replica when >5 requests are queued
          }
        },
        {
          # Secondary signal: KV-cache utilization approaching full => the
          # pod is near its real limit even if the queue is short.
          type = "prometheus"
          metadata = {
            serverAddress = "http://vllm-mistral-7b.${local.serving_namespace}.svc:80/metrics"
            query          = "max(vllm:gpu_cache_usage_perc)"
            threshold      = "0.85"
          }
        }
      ]
    }
  }

  depends_on = [helm_release.keda]
}

# --- Gateway API: GKE provisions the load balancer from this Gateway object.
# For internal-only serving swap the class to "gke-l7-rilb" and drop the
# public IP entirely.
resource "kubernetes_manifest" "inference_gateway" {
  manifest = {
    apiVersion = "gateway.networking.k8s.io/v1"
    kind       = "Gateway"
    metadata = {
      name      = "inference-gateway"
      namespace = kubernetes_namespace.serving.metadata[0].name
    }
    spec = {
      gatewayClassName = "gke-l7-global-external-managed"
      listeners = [
        {
          # Plain HTTP 80 for the demo; in prod use HTTPS 443 with
          # gateway.cert-manager.io/... or a Certificate Manager cert ref,
          # plus Cloud Armor policy attachment via GKEGatewayPolicy.
          name     = "http"
          port     = 80
          protocol = "HTTP"
          allowedRoutes = {
            namespaces = { from = "Same" }
          }
        }
      ]
    }
  }

  depends_on = [google_container_cluster.primary]
}

# --- HTTPRoute: model-based routing. Point the gateway at your router service
# in the real system; direct-to-vLLM here for the sample.
resource "kubernetes_manifest" "vllm_httproute" {
  manifest = {
    apiVersion = "gateway.networking.k8s.io/v1"
    kind       = "HTTPRoute"
    metadata = {
      name      = "vllm-mistral-7b"
      namespace = kubernetes_namespace.serving.metadata[0].name
    }
    spec = {
      parentRefs = [
        {
          name = kubernetes_manifest.inference_gateway.manifest.metadata.name
        }
      ]
      hostnames = ["inference.example.com"] # UPDATE ME; needs DNS to the GW IP
      rules = [
        {
          matches = [{ path = { type = "PathPrefix", value = "/" } }]
          backendRefs = [
            {
              name = kubernetes_service.vllm.metadata[0].name
              port = 80
            }
          ]
        }
      ]
    }
  }

  depends_on = [kubernetes_manifest.inference_gateway]
}

###############################################################################
# 12. OUTPUTS
###############################################################################

output "cluster_name" {
  value = google_container_cluster.primary.name
}

output "model_bucket" {
  value = google_storage_bucket.model_weights.url
}

output "artifact_registry" {
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.serving_images.repository_id}"
  description = "Push serving images here, e.g.: docker build -t $output . && docker push $output"
}

output "gateway_lb_ip_note" {
  value       = "Run: kubectl get gateway/${kubernetes_manifest.inference_gateway.manifest.metadata.name} -n model-serving -o jsonpath='{.status.addresses[0].value}'"
  description = "External IP of the inference Gateway — point DNS at it."
}
