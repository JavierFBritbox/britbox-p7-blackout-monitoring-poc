variable "env" {
  description = "Environment name (stage or prod)"
  type        = string

  validation {
    condition     = contains(["stage", "prod"], var.env)
    error_message = "env must be 'stage' or 'prod'."
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-1"
}

variable "lambda_memory" {
  description = "Lambda function memory in MB"
  type        = number
  default     = 512
}

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 30
}

variable "blackout_days" {
  description = "Number of days to check for blackout windows"
  type        = number
  default     = 30
}
