###############################################################################
# 0. TERRAFORM + PROVIDERS — the TRAINING layer
###############################################################################
# This stack implements the training/fine-tuning reference design on top of
# the serving infrastructure. It assumes the serving cluster + VPC already
# exist (looked up, not recreated) and adds:
#
#   - train-a3-spot   : H100 Spot node pool (checkpoint-restart makes Spot safe)
#   - train-dev       : L4 pool for LoRA / smoke tests / data-debug runs
#   - PriorityClasses : serving preempts training (the arbitration contract)
#   - GCS             : versioned datasets + lifecycle-tiered checkpoint store
#   - IAM             : least-privilege trainer SA via Workload Identity
#   - Kueue + JobSet  : gang scheduling, quotas, preemptible queues
#   - A sample multi-node training job wired to checkpoint + resume
#
# Two-phase apply on a fresh cluster — Kueue/JobSet CRDs must exist before
# the kubernetes_manifest resources that use them can plan:
#   terraform apply -target=helm_release.kueue
#   terraform apply
#
# NOTE on on-demand H100: this stack only creates the Spot pool. Deadline-
# bound runs burst onto on-demand A3 capacity — either a small dedicated
# pool with min_node_count=0, or (better) GKE's Dynamic Workload Scheduler
# via the google-beta provider's autoscaling { dynamic_workers {...} } block.
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
  description = "Existing GKE cluster (from the serving stack)."
  type        = string
  default     = "model-serving"
}

variable "vpc_name" {
  type    = string
  default = "model-serving-vpc"
}

locals {
  ns_train = "training"

  workload_pool = "${var.project_id}.svc.id.goog"

  # Bucket names must be globally unique — suffix with project ID.
  datasets_bucket    = "ml-datasets-${var.project_id}"
  checkpoints_bucket = "ml-checkpoints-${var.project_id}"
}

data "google_container_cluster" "existing" {
  name     = var.cluster_name
  location = var.region
}

data "google_compute_network" "vpc" {
  name = var.vpc_name
}

###############################################################################
# 2. NODE POOLS — tainted, GPU-dedicated, Spot-first
###############################################################################

# --- Flagship training pool: A3 (8x H100-80GB per node) on SPOT.
# Spot reclaims are a *design input*, not an emergency: preStop flushes a
# checkpoint (see the JobSet sample), the job auto-requeues, and resume
# picks up from the last trainer_state.json.
resource "google_container_node_pool" "train_a3_spot" {
  name     = "train-a3-spot"
  cluster  = data.google_container_cluster.existing.name
  location = var.region

  autoscaling {
    min_node_count = 0 # idle costs zero; Kueue admits jobs when capacity joins
    max_node_count = 16 # hard spend ceiling: 16 nodes = 128 H100s
  }

  management {
    auto_repair  = true
    auto_upgrade = true # upgrades preempt+restart nodes — checkpoints make this safe
  }

  node_config {
    machine_type = "a3-highgpu-8g"
    image_type   = "standard"

    guest_accelerator {
      type  = "nvidia-h100-80gb"
      count = 8
    }

    # Compact placement: pack nodes tightly for NCCL topology-awareness.
    # Inter-node all-reduce bandwidth is the difference between 35% MFU
    # and half that. Never run multi-node training without this.
    placement_policy {
      type = "COMPACT"
    }

    # Keep everything else off the training fleet.
    taint {
      key    = "workload"
      value  = "training"
      effect = "NO_SCHEDULE"
    }
    taint {
      # Spot taint: only checkpoint-safe workloads tolerate it.
      key    = "spot"
      value  = "true"
      effect = "NO_SCHEDULE"
    }

    labels = { pool = "train-a3-spot" }

    # GKE installs NVIDIA drivers automatically for GPU pools.
    # (Set gpu_driver_version = "DEFAULT" to pin explicitly if you have
    # NCCL/driver compatibility constraints — check the provider docs for
    # the exact accepted values for your GKE version.)
  }

  # A3 Spot machines are big; give CA a generous provisioning timeout.
  timeouts {
    create = "45m"
    update = "45m"
  }
}

# --- Dev/LoRA pool: single L4 machines for QLoRA, smoke tests, and
# data-debug runs. Cheap enough to leave warm during work hours.
resource "google_container_node_pool" "train_dev" {
  name     = "train-dev"
  cluster  = data.google_container_cluster.existing.name
  location = var.region

  autoscaling {
    min_node_count = 0
    max_node_count = 6
  }

  node_config {
    machine_type = "g2-standard-96"
    image_type   = "standard"

    guest_accelerator {
      type  = "nvidia-l4"
      count = 1
    }

    # NVMe scratch: stage dataset shards locally so dataloader reads don't
    # hammer GCS (the classic self-inflicted training bottleneck).
    ephemeral_storage_local_ssd_config {
      count = 1
    }

    taint {
      key    = "workload"
      value  = "training"
      effect = "NO_SCHEDULE"
    }

    labels = { pool = "train-dev" }
  }
}

###############################################################################
# 3. PRIORITY CLASSES — the serving-vs-training arbitration contract
###############################################################################
# Serving pods carry a high priority; training carries a low one. When the
# shared fleet comes under pressure, Kubernetes preempts low-priority pods.
# Kueue honors the same ordering inside its queues.
###############################################################################

resource "kubernetes_manifest" "priority_serving_high" {
  manifest = {
    apiVersion = "scheduling.k8s.io/v1"
    kind       = "PriorityClass"

    # value=false marks it non-preempting itself but still preemptible.
    metadata = { name = "serving-critical" }
    value    = 1000000
    globalDefault = false
    description  = "Production serving pods — preempt training, never preempted by it."
    preemptionPolicy = "Never" # serving pods don't need to preempt peers
  }
}

resource "kubernetes_manifest" "priority_training_low" {
  manifest = {
    apiVersion = "scheduling.k8s.io/v1"
    kind       = "PriorityClass"

    metadata = { name = "training-preemptible" }
    value    = 100 # far below serving: losing a training pod = a resume, not an outage
    globalDefault = false
    description  = "Preemptible training jobs — checkpoint before exit."
    preemptionPolicy = "Never" # training never preempts ANYONE (it requeues instead)
  }
}

###############################################################################
# 4. GCS — versioned datasets, lifecycle-tiered checkpoints
###############################################################################

# --- Datasets: immutable, content-addressed snapshots.
# Every training run pins an exact prefix (dataset hash). "Train on latest"
# is how reproducibility dies.
resource "google_storage_bucket" "datasets" {
  name                        = local.datasets_bucket
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  # Safety net: a botched re-preprocessing is recoverable.
  versioning {
    enabled = true
  }
}

# --- Checkpoints: hot first, tiered automatically.
resource "google_storage_bucket" "checkpoints" {
  name                        = local.checkpoints_bucket
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  # Checkpoints are immutable — no versioning, but never accidental-delete.
  lifecycle {
    prevent_destroy = true
  }

  # Tier: latest checkpoints stay hot for instant resumes; older ones decay.
  lifecycle_rule {
    condition {
      age            = 7
      matches_prefix = ["checkpoints/"] # only intermediate checkpoints
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  # Purge: 90 days of checkpoint history is plenty; final weights live in
  # the model registry / serving buckets, NOT here.
  lifecycle_rule {
    condition {
      age            = 90
      matches_prefix = ["checkpoints/"]
    }
    action {
      type = "Delete"
    }
  }
}

###############################################################################
# 5. IAM — the trainer identity, scoped to two buckets
###############################################################################

resource "google_service_account" "trainer" {
  account_id   = "ml-trainer"
  display_name = "Training jobs (JobSet / Ray)"
}

# Read datasets: read-only, never write — preprocessing jobs write via a
# separate identity you can audit.
resource "google_storage_bucket_iam_member" "trainer_read_datasets" {
  bucket = google_storage_bucket.datasets.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.trainer.email}"
}

# Checkpoints: read (resume) + write (save). Nothing else in the project.
resource "google_storage_bucket_iam_member" "trainer_write_checkpoints" {
  bucket = google_storage_bucket.checkpoints.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.trainer.email}"
}

# Pull training images from the registry created in the serving stack.
resource "google_project_iam_member" "trainer_pull_images" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.trainer.email}"
}

# ---------------------------------------------------------------------------
# OPTIONAL but recommended: Vertex AI TensorBoard, managed — no infra of your
# own to run for training dashboards. Requires the Vertex AI Service Agent to
# have access to your project's Artifact Registry (grant it
# roles/artifactregistry.reader if the first `tensorboard.create` complains).
# ---------------------------------------------------------------------------
resource "google_vertex_ai_tensorboard" "train_runs" {
  display_name = "training-runs"
  region       = var.region
  description  = "Managed TensorBoard for SFT/LoRA run tracking."
}

###############################################################################
# 6. KUBERNETES PROVIDER WIRING
###############################################################################

data "google_client_config" "default" {}

provider "kubernetes" {
  host  = "https://${data.google_container_cluster.existing.endpoint}"
  token = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(
    data.google_container_cluster.existing.master_auth[0].cluster_ca_certificate,
  )
}

provider "helm" {
  kubernetes {
    host  = "https://${data.google_container_cluster.existing.endpoint}"
    token = data.google_client_config.default.access_token
    cluster_ca_certificate = base64decode(
      data.google_container_cluster.existing.master_auth[0].cluster_ca_certificate,
    )
  }
}

###############################################################################
# 7. KUEUE — gang scheduling, quotas, preemption policies
###############################################################################
# Kueue: multi-node NCCL jobs need ALL their GPUs at once or nothing —
# vanilla sequential scheduling deadlocks them. Kueue admits whole "gangs",
# enforces per-team quotas, and makes preemption a first-class, queue-scoped
# operation. (Alternative: Ray/KubeRay if your team prefers elastic,
# Python-first training; the pools and buckets here stay identical.)
###############################################################################

resource "kubernetes_namespace" "training" {
  metadata {
    name = local.ns_train

    labels = {
      # Kueue's default allowlist admits jobs into any namespace by default;
      # label-based namespacing keeps teams separated in real deployments.
      "kueue-manager" = "enabled"
    }
  }
}

resource "helm_release" "kueue" {
  name       = "kueue"
  namespace  = "kueue-system"
  repository = "oci://us-docker.pkg.dev/kueue-charts/charts" # official OCI chart
  chart      = "kueue"
  version    = "0.11.0" # ships the JobSet CRDs it manages — pin in prod

  depends_on = [google_container_node_pool.train_a3_spot]
}

# --- ResourceFlavor: a label Kueue matches to this Spot pool.
resource "kubernetes_manifest" "flavor_a3_spot" {
  manifest = {
    apiVersion = "kueue.x-k8s.io/v1beta1"
    kind       = "ResourceFlavor"

    metadata = { name = "train-a3-spot" }
    spec = {
      # Match the pool labels from section 2.
      nodeSelector = {
        pool = "train-a3-spot"
      }
      tolerations = [
        {
          key      = "workload"
          operator = "Equal"
          value    = "training"
          effect   = "NoSchedule"
        },
        {
          key      = "spot"
          operator = "Equal"
          value    = "true"
          effect   = "NoSchedule"
        },
      ]
    }
  }

  depends_on = [helm_release.kueue]
}

# --- ClusterQueue: the quota + preemption policy for training.
resource "kubernetes_manifest" "cluster_queue_training" {
  manifest = {
    apiVersion = "kueue.x-k8s.io/v1beta1"
    kind       = "ClusterQueue"

    metadata = { name = "training-queue" }
    spec = {
      # Cohort: queues in a cohort can BORROW each other's unused quota.
      # Put the serving queue in the same cohort and serving always wins
      # (priority classes), while training borrows serving's idle GPUs.
      cohort = "gpu-fleet"

      namespaceSelector = {
        matchLabels = { "kubernetes.io/metadata.name" = local.ns_train }
      }

      # Spend ceiling: nominal is guaranteed borrowing, borrowing is burst.
      nominalQuota = [{
        name     = "h100-spot"
        flavor   = kubernetes_manifest.flavor_a3_spot.manifest.metadata.name
        resources = [
          { name = "cpu",              nominalQuota = "256" },
          { name = "memory",           nominalQuota = "2Ti" },
          { name = "nvidia.com/gpu",   nominalQuota = "64" } # 8 nodes' worth
        ]
      }]

      # Preemption within the queue: requeue jobs when a higher-priority
      # (deadline-bound) run needs the GPUs.
      preemption = {
        withinClusterQueue = "LowerPriority"
        reclaimWithinCohort = "Never" # serving reclaims its OWN quota via k8s preemption
      }
    }
  }

  depends_on = [helm_release.kueue]
}

# --- LocalQueue: the per-team handle jobs point at.
resource "kubernetes_manifest" "local_queue_training" {
  manifest = {
    apiVersion = "kueue.x-k8s.io/v1beta1"
    kind       = "LocalQueue"

    metadata = {
      name      = "training"
      namespace = kubernetes_namespace.training.metadata[0].name
    }
    spec = {
      clusterQueue = "training-queue"
    }
  }

  depends_on = [kubernetes_manifest.cluster_queue_training]
}

# --- Workload Identity for the trainer pods.
resource "kubernetes_service_account" "trainer" {
  metadata {
    name      = "ml-trainer"
    namespace = kubernetes_namespace.training.metadata[0].name
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.trainer.email
    }
  }
}

resource "google_service_account_iam_member" "wi_trainer" {
  service_account_id = google_service_account.trainer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${local.workload_pool}[${local.ns_train}/ml-trainer]"
}

###############################################################################
# 8. SAMPLE TRAINING JOB — a preemptible, checkpoint-safe JobSet
###############################################################################
# Multi-node SFT: 2x A3 nodes (16 H100s), torchrun across both, checkpointing
# to GCS every N steps. This is the shape every run follows:
#   - suspend: true        => Kueue controls admission (the gang)
#   - kueue queue-name     => which LocalQueue
#   - preStop flush        => SIGTERM means "save and exit", not "die"
#   - trainer_state.json   => resume replays the exact data cursor
# Two-phase apply: the JobSet CRD ships with the Kueue chart (section 7).
###############################################################################

resource "kubernetes_manifest" "train_job_sft" {
  manifest = {
    apiVersion = "jobset.x-k8s.io/v1alpha2"
    kind       = "JobSet"

    metadata = {
      name      = "sft-70b-v42"
      namespace = kubernetes_namespace.training.metadata[0].name

      annotations = {
        # The LocalQueue from section 7 — Kueue suspends the JobSet until
        # the whole gang (both nodes, all 16 GPUs) fits atomically.
        "kueue.x-k8s.io/queue-name" = "training"
      }
    }

    spec = {
      # suspend: true is set by Kueue on admission; explicit here so a
      # pre-apply of this manifest never side-schedules anything.
      suspend = true

      # On preemption (Spot reclaim or serving scale-up): requeue and keep
      # the same run identity — resume loads trainer_state.json + weights.
      replicatedJobs = [
        {
          name     = "workers"
          replicas = 2 # 2x nodes; each job replica = one a3-highgpu-8g node
          template = {
            spec = {
              # Restart in-place on node auto-repair (same gang); full
              # requeue happens only on preemption/eviction.
              backoffLimit = 0
              template = {
                spec = {
                  serviceAccountName = kubernetes_service_account.trainer.metadata[0].name
                  priorityClassName  = "training-preemptible"

                  # Grace: the preStop flush needs time to finish writing
                  # the checkpoint before SIGKILL.
                  terminationGracePeriodSeconds = 600

                  # The init stage pulls dataset shards to LocalSSD first —
                  # random GCS reads during training are the classic bottleneck.
                  initContainers = [
                    {
                      name  = "stage-data"
                      image = "google/cloud-sdk:slim"
                      command = [
                        "sh", "-c",
                        # rsync the pinned, content-addressed snapshot only.
                        "gsutil -m cp -r ${google_storage_bucket.datasets.url}/snapshots/$DATASET_HASH/ /data/"
                      ]
                      env = [
                        {
                          name  = "DATASET_HASH"
                          value = "sha256:REPLACE-WITH-DATASET-SNAPSHOT-HASH" # set per run by CI
                        }
                      ]
                      volumeMounts = [{ name = "scratch", mountPath = "/data" }]
                    }
                  ]

                  containers = [
                    {
                      name  = "trainer"
                      image = "us-central1-docker.pkg.dev/REPLACE-PROJECT/model-serving/train:v0.1.0" # UPDATE ME

                      # torchrun bootstraps the NCCL process group across
                      # the gang; RANK/WORLD_SIZE are injected by JobSet.
                      command = [
                        "torchrun",
                        "--nnodes=2", "--nproc-per-node=8",
                        "train_sft.py"
                      ]
                      env = [
                        {
                          name  = "CHECKPOINT_DIR"
                          value = "gs://${google_storage_bucket.checkpoints.name}/checkpoints/sft-70b-v42"
                        },
                        {
                          name  = "DATA_DIR"
                          value = "/data"
                        },
                        {
                          # Compact placement is set at the pool level; this
                          # just tunes NCCL for the tight topology.
                          name  = "NCCL_DEBUG"
                          value = "WARN"
                        }
                      ]

                      # Checkpoint cadence is a cost decision: never more
                      # lost work than you'd accept. 10 min of H100 time on
                      # 16 GPUs ≈ $5-10 — set the interval accordingly.
                      resources = {
                        requests = {
                          cpu              = "32"
                          memory           = "256Gi"
                          "nvidia.com/gpu" = "8"
                        }
                        limits = {
                          "nvidia.com/gpu" = "8"
                        }
                      }

                      # THE preemption-safety line: SIGTERM => flush a final
                      # checkpoint before exit. Without this, Spot is reckless.
                      lifecycle = {
                        preStop = {
                          exec = {
                            command = ["sh", "-c", "python flush_checkpoint.py"]
                          }
                        }
                      }

                      volumeMounts = [{ name = "scratch", mountPath = "/data" }]
                    }
                  ]

                  volumes = [
                    {
                      name = "scratch"
                      emptyDir = {} # backed by the node's LocalSSD (ephemeral pool config)
                    }
                  ]

                  nodeSelector = { pool = "train-a3-spot" }
                  tolerations = [
                    { key = "workload", operator = "Equal", value = "training", effect = "NoSchedule" },
                    { key = "spot",     operator = "Equal", value = "true",      effect = "NoSchedule" }
                  ]
                }
              }
            }
          }
        }
      ]
    }
  }

  depends_on = [helm_release.kueue, kubernetes_manifest.local_queue_training]
}

###############################################################################
# 9. OUTPUTS
###############################################################################

output "checkpoint_bucket" {
  description = "Where runs checkpoint + resume from (lifecycle-tiered)."
  value       = google_storage_bucket.checkpoints.url
}

output "datasets_bucket" {
  description = "Versioned dataset snapshot store — pin per-run prefixes by hash."
  value       = google_storage_bucket.datasets.url
}

output "tensorboard_url" {
  value = google_vertex_ai_tensorboard.train_runs.name
}

output "queue_note" {
  description = "The arbitration contract in one line."
  value       = "Serving = priorityClass serving-critical; training = training-preemptible. Spot reclaims and serving scale-ups requeue the job; resume loads trainer_state.json — preemption costs one checkpoint interval, not the run."
}
