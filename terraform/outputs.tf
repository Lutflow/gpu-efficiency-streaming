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
