# ===========================================================================
# flink.tf -- Flink SQL statements, managed as code.
# Each statement's text is read from flink/*.sql via file(), so the SQL in the
# repo is the single source of truth. depends_on enforces submission order:
#   add_event_time -> set_watermark -> detect_anomalies -> alerts
#   add_event_time -> set_watermark -> forecast -> capacity_risk
# ===========================================================================

locals {
  flink_properties = {
    "sql.current-catalog"  = confluent_environment.demo.display_name
    "sql.current-database" = confluent_kafka_cluster.demo.display_name
  }
}

resource "confluent_flink_statement" "add_event_time" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.demo.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.demo.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement_name = "gpu-efficiency-01a-add-event-time"
  statement      = file("${path.module}/../flink/01a_add_event_time.sql")
  properties     = local.flink_properties
  rest_endpoint  = data.confluent_flink_region.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.app_flink.id
    secret = confluent_api_key.app_flink.secret
  }

  depends_on = [confluent_subject_config.telemetry_value]
}

resource "confluent_flink_statement" "set_watermark" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.demo.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.demo.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement_name = "gpu-efficiency-01b-set-watermark"
  statement      = file("${path.module}/../flink/01b_set_watermark.sql")
  properties     = local.flink_properties
  rest_endpoint  = data.confluent_flink_region.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.app_flink.id
    secret = confluent_api_key.app_flink.secret
  }

  depends_on = [confluent_flink_statement.add_event_time]
}

resource "confluent_flink_statement" "detect_anomalies" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.demo.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.demo.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement_name = "gpu-efficiency-02-detect-anomalies"
  statement      = file("${path.module}/../flink/02_detect_anomalies.sql")
  properties     = local.flink_properties
  rest_endpoint  = data.confluent_flink_region.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.app_flink.id
    secret = confluent_api_key.app_flink.secret
  }

  depends_on = [confluent_flink_statement.set_watermark]
}

resource "confluent_flink_statement" "alerts" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.demo.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.demo.id
  }
  principal {
    id = confluent_service_account.app.id
  }

  statement_name = "gpu-efficiency-03-alerts"
  statement      = file("${path.module}/../flink/03_alerts.sql")
  properties     = local.flink_properties
  rest_endpoint  = data.confluent_flink_region.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.app_flink.id
    secret = confluent_api_key.app_flink.secret
  }

  depends_on = [confluent_flink_statement.detect_anomalies]
}


resource "confluent_flink_statement" "forecast" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.demo.id }
  compute_pool { id = confluent_flink_compute_pool.demo.id }
  principal { id = confluent_service_account.app.id }

  statement_name = "gpu-efficiency-05-forecast"
  statement      = file("${path.module}/../flink/05_forecast.sql")
  properties     = local.flink_properties
  rest_endpoint  = data.confluent_flink_region.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.app_flink.id
    secret = confluent_api_key.app_flink.secret
  }

  depends_on = [
    confluent_flink_statement.set_watermark,
    confluent_role_binding.app_topic,
    confluent_role_binding.app_subject,
  ]
}

resource "confluent_flink_statement" "capacity_risk" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.demo.id }
  compute_pool { id = confluent_flink_compute_pool.demo.id }
  principal { id = confluent_service_account.app.id }

  statement_name = "gpu-efficiency-07-capacity-risk"
  statement      = file("${path.module}/../flink/07_capacity_risk.sql")
  properties     = local.flink_properties
  rest_endpoint  = data.confluent_flink_region.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.app_flink.id
    secret = confluent_api_key.app_flink.secret
  }

  depends_on = [confluent_flink_statement.forecast]
}
