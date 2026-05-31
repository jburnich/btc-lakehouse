resource "aws_glue_catalog_database" "main" {
  name        = "btc_lakehouse"
  description = "Bitcoin on-chain analytics"
}
