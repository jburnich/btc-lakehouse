output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_execution_role_arn" {
  value = aws_iam_role.emr_execution.arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.main.name
}

output "streamlit_access_key_id" {
  value     = aws_iam_access_key.streamlit.id
  sensitive = true
}

output "streamlit_secret_access_key" {
  value     = aws_iam_access_key.streamlit.secret
  sensitive = true
}
