# ---------------------------------------------------------------------------
# Confluent Cloud control-plane credentials (Cloud API key, NOT a cluster key).
# Provide via terraform.tfvars (gitignored) or TF_VAR_ environment variables.
# ---------------------------------------------------------------------------
variable "confluent_cloud_api_key" {
  type        = string
  description = "Confluent Cloud API key (control plane)."
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  type        = string
  description = "Confluent Cloud API secret (control plane)."
  sensitive   = true
}

# ---------------------------------------------------------------------------
# Cloud / region for the Kafka cluster and Flink compute pool.
# ---------------------------------------------------------------------------
variable "cloud_provider" {
  type        = string
  description = "Cloud provider for the Confluent cluster + Flink region (AWS, GCP, or AZURE)."
  default     = "AWS"
}

variable "region" {
  type        = string
  description = "Cloud region for the Kafka cluster and Flink compute pool (e.g. us-east-1)."
  default     = "us-east-1"
}

variable "environment_name" {
  type        = string
  description = "Display name for the Confluent environment."
  default     = "gpu-efficiency-demo"
}

# ---------------------------------------------------------------------------
# Amazon S3 Sink (committed core sink) -- the efficiency data lake.
# ---------------------------------------------------------------------------
variable "aws_access_key_id" {
  type        = string
  description = "AWS access key id for the S3 Sink connector."
  sensitive   = true
}

variable "aws_secret_access_key" {
  type        = string
  description = "AWS secret access key for the S3 Sink connector."
  sensitive   = true
}

variable "s3_bucket_name" {
  type        = string
  description = "Target S3 bucket for the efficiency data lake."
}

variable "aws_region" {
  type        = string
  description = "AWS region of the S3 bucket."
  default     = "us-east-1"
}
