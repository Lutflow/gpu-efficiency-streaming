# ===========================================================================
# outputs.tf
# ===========================================================================

output "environment_id" {
  description = "Confluent environment id."
  value       = confluent_environment.demo.id
}

output "kafka_cluster_id" {
  description = "Confluent Kafka cluster id."
  value       = confluent_kafka_cluster.demo.id
}

output "flink_compute_pool_id" {
  description = "Flink compute pool id."
  value       = confluent_flink_compute_pool.demo.id
}

output "telemetry_topic" {
  description = "Source telemetry topic."
  value       = confluent_kafka_topic.telemetry.topic_name
}

# The screenshot the Confluent Laptop Challenge form asks for:
# Source -> Flink -> Flink -> Sink, rendered live in Stream Lineage.
output "stream_lineage_url" {
  description = "Open this to capture the Stream Lineage screenshot for the challenge form."
  value       = "https://confluent.cloud/environments/${confluent_environment.demo.id}/clusters/${confluent_kafka_cluster.demo.id}/stream-lineage"
}

# --- Producer credentials for `uv run produce` (sensitive) ---
output "bootstrap_servers" {
  description = "Kafka bootstrap endpoint for the producer."
  value       = confluent_kafka_cluster.demo.bootstrap_endpoint
}

output "schema_registry_url" {
  description = "Schema Registry REST endpoint for the producer."
  value       = data.confluent_schema_registry_cluster.sr.rest_endpoint
}

output "producer_kafka_api_key" {
  description = "Kafka API key for the producer (service account)."
  value       = confluent_api_key.app_kafka.id
  sensitive   = true
}

output "producer_kafka_api_secret" {
  value     = confluent_api_key.app_kafka.secret
  sensitive = true
}

output "producer_sr_api_key" {
  description = "Schema Registry API key for the producer (service account)."
  value       = confluent_api_key.app_sr.id
  sensitive   = true
}

output "producer_sr_api_secret" {
  value     = confluent_api_key.app_sr.secret
  sensitive = true
}
