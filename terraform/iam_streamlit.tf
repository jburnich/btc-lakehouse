resource "aws_iam_user" "streamlit" {
  name = "btc-streamlit"
}

resource "aws_iam_access_key" "streamlit" {
  user = aws_iam_user.streamlit.name
}

resource "aws_iam_user_policy" "streamlit_athena" {
  name = "btc-streamlit-athena"
  user = aws_iam_user.streamlit.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = "arn:aws:athena:${var.aws_region}:*:workgroup/${var.athena_workgroup}"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.main.arn,
          "${aws_s3_bucket.main.arn}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
        ]
        Resource = ["*"]
      },
    ]
  })
}
