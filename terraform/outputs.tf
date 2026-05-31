output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_execution_role_arn" {
  value = aws_iam_role.emr_execution.arn
}
