output "function_url" {
  description = "Public URL for the blackout monitoring Lambda"
  value       = aws_lambda_function_url.blackout_url.function_url
}

output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.blackout_monitor.function_name
}

output "lambda_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_role.arn
}
