resource "aws_athena_workgroup" "main" {
  name = var.athena_workgroup

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_name}/athena-results/"
    }
  }
}
