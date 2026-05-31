resource "aws_emrserverless_application" "spark" {
  name          = "btc-lakehouse"
  release_label = "emr-7.12.0"
  type          = "spark"

  maximum_capacity {
    cpu    = "20 vCPU"
    memory = "40 GB"
    disk   = "100 GB"
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}
