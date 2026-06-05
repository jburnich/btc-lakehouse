variable "bucket_name" {
  description = "Main S3 bucket name"
  type        = string
}

variable "aws_region" {
  type = string
}

variable "athena_workgroup" {
  description = "Athena workgroup name"
  type        = string
}
