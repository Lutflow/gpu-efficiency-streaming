# ===========================================================================
# connectors.tf -- fully managed Amazon S3 sinks (governed multi-destination).
# The telemetry SOURCE is the custom producer (`uv run produce`), not a connector.
# Secrets flow through config_sensitive (never persisted to the repo).
# ===========================================================================

# --- Amazon S3 Sink: the efficiency data lake (alerts -> FinOps / reconciliation) ---
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

# --- Amazon S3 Sink: raw telemetry archive (replay / offline training) ---
# Branches the SOURCE topic: the raw governed stream is also landed to cheap storage,
# in parallel with the real-time detect/forecast paths -- standard lakehouse pattern.
resource "confluent_connector" "s3_raw_archive" {
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
    "name"                     = "gpu-telemetry-raw-archive"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app.id
    "topics"                   = confluent_kafka_topic.telemetry.topic_name
    "input.data.format"        = "AVRO"
    "output.data.format"       = "JSON"
    "s3.bucket.name"           = var.s3_bucket_name
    "s3.region"                = var.aws_region
    "topics.dir"               = "gpu-telemetry-raw"
    "time.interval"            = "HOURLY"
    "flush.size"               = "1000"
    "tasks.max"                = "1"
  }

  depends_on = [confluent_subject_config.telemetry_value]
}

# --- Amazon S3 Sink: predictive capacity-risk output (PREDICTED_IDLE) ---
# Closes the capacity-risk branch into a governed destination (FinOps planning lake).
resource "confluent_connector" "s3_capacity_risk" {
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
    "name"                     = "gpu-capacity-risk-s3-sink"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app.id
    "topics"                   = "gpu_efficiency_capacity_risk"
    "input.data.format"        = "AVRO"
    "output.data.format"       = "JSON"
    "s3.bucket.name"           = var.s3_bucket_name
    "s3.region"                = var.aws_region
    "topics.dir"               = "gpu-capacity-risk"
    "time.interval"            = "HOURLY"
    "flush.size"               = "1000"
    "tasks.max"                = "1"
  }

  depends_on = [confluent_flink_statement.capacity_risk]
}
