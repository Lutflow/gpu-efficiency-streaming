# ===========================================================================
# main.tf -- core Confluent Cloud infrastructure
# environment + Kafka cluster + Schema Registry + Flink compute pool
# + service account + RBAC + API keys + source topic + Avro schema
# ===========================================================================

data "confluent_organization" "main" {}

# --- Environment with Stream Governance (Schema Registry + Stream Lineage) ---
resource "confluent_environment" "demo" {
  display_name = var.environment_name

  stream_governance {
    package = "ESSENTIALS"
  }
}

data "confluent_schema_registry_cluster" "sr" {
  environment {
    id = confluent_environment.demo.id
  }

  depends_on = [confluent_kafka_cluster.demo]
}

# --- Standard Kafka cluster (single zone -- lowest cost that supports topic-scoped RBAC) ---
resource "confluent_kafka_cluster" "demo" {
  display_name = "gpu-efficiency"
  availability = "SINGLE_ZONE"
  cloud        = var.cloud_provider
  region       = var.region

  # Standard tier: supports resource-scoped (topic-level) RBAC role bindings, which
  # Basic does not. This is the realistic production tier and lets us keep the
  # least-privilege, topic-scoped grants below.
  standard {}

  environment {
    id = confluent_environment.demo.id
  }
}

# --- Flink compute pool (runs ML_DETECT_ANOMALIES inline) ---
resource "confluent_flink_compute_pool" "demo" {
  display_name = "gpu-efficiency-pool"
  cloud        = var.cloud_provider
  region       = var.region
  max_cfu      = 10

  environment {
    id = confluent_environment.demo.id
  }
}

data "confluent_flink_region" "demo" {
  cloud  = var.cloud_provider
  region = var.region
}

# --- Service account used by connectors, schema management, and Flink ---
resource "confluent_service_account" "app" {
  display_name = "gpu-efficiency-app"
  description  = "Service account for the GPU efficiency streaming demo pipeline"
}

# ---------------------------------------------------------------------------
# RBAC: least privilege (Lutflow production posture).
# No CloudClusterAdmin / EnvironmentAdmin. The service account gets only:
#   - data-plane Developer roles scoped to the specific pipeline topics
#   - Schema Registry read/write (to register and read the pipeline subjects)
#   - FlinkDeveloper on the environment (to submit statements)
# ---------------------------------------------------------------------------
locals {
  # All topics this pipeline touches (source + Flink CTAS outputs).
  pipeline_topics = [
    "gpu_telemetry",                # source (produced to by the producer; read by Flink)
    "gpu_efficiency_anomalies",     # Flink CTAS output (detect)
    "gpu_efficiency_alerts",        # Flink CTAS output (alert rule)
    "gpu_efficiency_forecast",      # Flink CTAS output (forecast)
    "gpu_efficiency_capacity_risk", # Flink CTAS output (predicted idle)
    "gpu_efficiency_waste",         # Flink CTAS output (utilization-lies waste detector)
    "gpu_remediation",              # Flink CTAS output (rule-based remediation recommender)
  ]
  # ResourceOwner scoped to each specific pipeline topic: covers create/alter/produce/consume
  # (Flink's ALTER TABLE needs ownership of the underlying topic). Still least-privilege --
  # the service account owns only these pipeline topics, nothing else on the cluster.
  topic_roles = ["ResourceOwner"]
  topic_role_bindings = {
    for pair in setproduct(local.pipeline_topics, local.topic_roles) :
    "${pair[0]}::${pair[1]}" => { topic = pair[0], role = pair[1] }
  }
}

resource "confluent_role_binding" "app_topic" {
  for_each = local.topic_role_bindings

  principal   = "User:${confluent_service_account.app.id}"
  role_name   = each.value.role
  crn_pattern = "${confluent_kafka_cluster.demo.rbac_crn}/kafka=${confluent_kafka_cluster.demo.id}/topic=${each.value.topic}"
}

# Schema Registry: subject-scoped access (SR developer/owner roles bind at the
# subject scope, not the SR-cluster scope). ResourceOwner on each pipeline subject
# lets the service account register, read, and set compatibility on just those
# subjects -- no EnvironmentAdmin.
resource "confluent_role_binding" "app_subject" {
  for_each = toset([
    "gpu_telemetry-value",
    "gpu_efficiency_anomalies-value", "gpu_efficiency_anomalies-key",
    "gpu_efficiency_alerts-value", "gpu_efficiency_alerts-key",
    "gpu_efficiency_forecast-value", "gpu_efficiency_forecast-key",
    "gpu_efficiency_capacity_risk-value", "gpu_efficiency_capacity_risk-key",
    "gpu_efficiency_waste-value", "gpu_efficiency_waste-key",
    "gpu_remediation-value", "gpu_remediation-key",
  ])

  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.sr.resource_name}/subject=${each.value}"
}

# Flink: submit statements in this environment.
resource "confluent_role_binding" "app_flink_developer" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_environment.demo.resource_name
}

# Flink writes to result topics with an exactly-once (transactional) producer, so the
# principal needs access to transactional IDs. Sinks/Flink consumers need consumer-group
# access. Both are scoped to this cluster (still no CloudClusterAdmin).
resource "confluent_role_binding" "app_txn_id" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${confluent_kafka_cluster.demo.rbac_crn}/kafka=${confluent_kafka_cluster.demo.id}/transactional-id=*"
}

resource "confluent_role_binding" "app_group" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${confluent_kafka_cluster.demo.rbac_crn}/kafka=${confluent_kafka_cluster.demo.id}/group=*"
}

# Managed sink connectors auto-create a dead-letter-queue topic named dlq-<connector-id>.
# Grant the service account ownership of the dlq-* prefix so the S3 sink can create/write it.
resource "confluent_role_binding" "app_dlq" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${confluent_kafka_cluster.demo.rbac_crn}/kafka=${confluent_kafka_cluster.demo.id}/topic=dlq-*"
}

# --- Kafka API key (topic + connector management) ---
resource "confluent_api_key" "app_kafka" {
  display_name = "gpu-efficiency-kafka-key"
  description  = "Kafka API key for the GPU efficiency demo service account"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.demo.id
    api_version = confluent_kafka_cluster.demo.api_version
    kind        = confluent_kafka_cluster.demo.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  depends_on = [confluent_role_binding.app_topic]
}

# --- Schema Registry API key ---
resource "confluent_api_key" "app_sr" {
  display_name = "gpu-efficiency-sr-key"
  description  = "Schema Registry API key for the GPU efficiency demo service account"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  depends_on = [confluent_role_binding.app_subject]
}

# --- Flink API key (statement submission) ---
resource "confluent_api_key" "app_flink" {
  display_name = "gpu-efficiency-flink-key"
  description  = "Flink API key for the GPU efficiency demo service account"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.demo.id
    api_version = data.confluent_flink_region.demo.api_version
    kind        = data.confluent_flink_region.demo.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  depends_on = [confluent_role_binding.app_flink_developer]
}

# --- Source topic: gpu_telemetry (Avro, governed by Schema Registry) ---
resource "confluent_kafka_topic" "telemetry" {
  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  topic_name       = "gpu_telemetry"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.demo.rest_endpoint

  # Explicit topic config (Lutflow convention): streaming source, delete cleanup,
  # short retention for a demo (1 day). Adjust for a real fleet.
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "86400000" # 1 day
  }

  credentials {
    key    = confluent_api_key.app_kafka.id
    secret = confluent_api_key.app_kafka.secret
  }
}

# ---------------------------------------------------------------------------
# Governed schema (ADR-046 style): the canonical Avro schema is registered here
# (Terraform is the registrant -- NOT connector auto-registration), and the subject
# compatibility is pinned to BACKWARD. The producer (uv run produce) produces the identical
# base schema, so no incompatible re-registration occurs.
#
# CLI equivalent (documented in the README) -- run before `terraform apply` if you
# prefer to govern the subject out-of-band:
#   confluent schema-registry schema create \
#     --subject gpu_telemetry-value --schema schemas/gpu_telemetry.avsc --type avro
#   confluent schema-registry subject update gpu_telemetry-value --compatibility BACKWARD
# ---------------------------------------------------------------------------
resource "confluent_schema" "telemetry_value" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint

  subject_name = "${confluent_kafka_topic.telemetry.topic_name}-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/gpu_telemetry.avsc")

  credentials {
    key    = confluent_api_key.app_sr.id
    secret = confluent_api_key.app_sr.secret
  }
}

resource "confluent_subject_config" "telemetry_value" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint

  subject_name        = confluent_schema.telemetry_value.subject_name
  compatibility_level = "BACKWARD"

  credentials {
    key    = confluent_api_key.app_sr.id
    secret = confluent_api_key.app_sr.secret
  }
}
