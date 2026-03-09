################################################################################
# IAM Role
################################################################################

resource "aws_iam_role" "lambda_role" {
  name = "p7-blackout-monitoring-poc-${var.env}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

################################################################################
# S3 Read Policy
################################################################################

resource "aws_iam_policy" "s3_read" {
  name        = "p7-blackout-monitoring-poc-${var.env}-s3-read"
  description = "Allow read access to P7 EPG schedule output bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = "arn:aws:s3:::britbox-epg-schedule-p7-output-${var.env}"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = "arn:aws:s3:::britbox-epg-schedule-p7-output-${var.env}/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "s3_read" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_read.arn
}

################################################################################
# CloudWatch Log Group
################################################################################

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/p7-blackout-monitoring-poc-${var.env}"
  retention_in_days = 14
}

################################################################################
# Lambda Function
################################################################################

resource "aws_lambda_function" "blackout_monitor" {
  function_name    = "p7-blackout-monitoring-poc-${var.env}"
  role             = aws_iam_role.lambda_role.arn
  handler          = "index.handler"
  runtime          = "nodejs20.x"
  memory_size      = var.lambda_memory
  timeout          = var.lambda_timeout
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      OUTPUT_BUCKET = "britbox-epg-schedule-p7-output-${var.env}"
      BLACKOUT_DAYS = tostring(var.blackout_days)
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic,
    aws_cloudwatch_log_group.lambda_logs,
  ]
}

################################################################################
# Lambda Function URL
################################################################################

resource "aws_lambda_function_url" "blackout_url" {
  function_name      = aws_lambda_function.blackout_monitor.function_name
  authorization_type = "NONE"

  cors {
    allow_methods = ["GET"]
    allow_origins = ["*"]
  }
}
