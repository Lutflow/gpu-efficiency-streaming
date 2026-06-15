# ===========================================================================
# connectors.tf -- fully managed Confluent connectors
# Committed: Datagen Source + Amazon S3 Sink.
# Optional showcase: HTTP Sink, Datadog Metrics Sink (toggled by variables).
# All secrets flow through config_sensitive (never persisted to the repo).
# ===========================================================================

# --- Datagen Source: synthetic, standards-grounded GPU telemetry ---
resource "confluent_connector" "datagen_source" {
  environment {
    id = confluent_environment.demo.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  config_nonsensitive = {
    "connector.class"          = "DatagenSource"
    "name"                     = "gpu-telemetry-datagen"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app.id
    "kafka.topic"              = confluent_kafka_topic.telemetry.topic_name
    "output.data.format"       = "AVRO"
    "schema.string"            = file("${path.module}/../scripts/datagen_schema.json")
    "max.interval"             = "1000"
    "tasks.max"                = "1"
  }

  # The canonical schema is registered + pinned to BACKWARD by Terraform first
  # (confluent_schema/confluent_subject_config). The Datagen Source then produces
  # the identical base schema, so no incompatible re-registration occurs.
  depends_on = [confluent_subject_config.telemetry_value]
}

# --- Amazon S3 Sink: the efficiency data lake (FinOps / billing reconciliation) ---
resource "confluent_connector" "s3_sink" {
  environment {
    id = confluent_environment.demo.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  config_sensitive = {
    "aws.access.key.id"     = var.aws_access_key_id
    "aws.secret.access.key" = var.aws_secret_access_key
  }

  config_nonsensitive = {
    "connector.class"          = "S3_SINK"
    "name"                     = "gpu-efficiency-s3-sink"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app.id
    "topics"                   = "gpu_efficiency_alerts"
    "input.data.format"        = "AVRO"
    "output.data.format"       = "JSON"
    "s3.bucket.name"           = var.s3_bucket_name
    "s3.region"                = var.aws_region
    "topics.dir"               = "gpu-efficiency-lake"
    "time.interval"            = "HOURLY"
    "flush.size"               = "1000"
    "tasks.max"                = "1"
  }

  depends_on = [confluent_flink_statement.alerts]
}

# --- HTTP Sink (optional): POST each anomaly to an alert webhook ---
resource "confluent_connector" "http_sink" {
  count = var.enable_http_sink ? 1 : 0

  environment {
    id = confluent_environment.demo.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  config_nonsensitive = {
    "connector.class"          = "HttpSink"
    "name"                     = "gpu-efficiency-alerts-http"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app.id
    "topics"                   = "gpu_efficiency_alerts"
    "http.api.url"             = var.alert_webhook_url
    "request.method"           = "POST"
    "input.data.format"        = "JSON"
    "behavior.on.error"        = "IGNORE"
    "tasks.max"                = "1"
  }

  depends_on = [confluent_flink_statement.alerts]
}

# --- Datadog Metrics Sink (optional): real-time efficiency observability ---
resource "confluent_connector" "datadog_sink" {
  count = var.enable_datadog_sink ? 1 : 0

  environment {
    id = confluent_environment.demo.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  config_sensitive = {
    "datadog.api.key" = var.datadog_api_key
  }

  config_nonsensitive = {
    "connector.class"          = "DatadogMetricsSink"
    "name"                     = "gpu-efficiency-datadog-sink"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app.id
    "topics"                   = "gpu_efficiency_datadog"
    "input.data.format"        = "AVRO"
    "datadog.domain"           = var.datadog_site
    "max.retry.time.ms"        = "30000"
    "tasks.max"                = "1"
  }

  depends_on = [confluent_flink_statement.datadog_metrics]
}
