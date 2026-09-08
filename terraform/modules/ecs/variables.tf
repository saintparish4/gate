variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "public_subnet_ids" {
  type = list(string)
}

variable "container_image" {
  type = string
}

variable "container_port" {
  type    = number
  default = 8080
}

variable "desired_count" {
  type    = number
  default = 2
}

variable "cpu" {
  type    = number
  default = 256
}

variable "memory" {
  type    = number
  default = 512
}

variable "environment_variables" {
  type    = map(string)
  default = {}
}

variable "dynamodb_table_arns" {
  type = list(string)
}

variable "kinesis_stream_arn" {
  type = string
}

variable "secrets_manager_arn" {
  type = string
}

variable "enable_autoscaling" {
  description = "Enable autoscaling for the ECS service"
  type        = bool
  default     = true
}

variable "min_capacity" {
  type    = number
  default = 2
}

variable "max_capacity" {
  type    = number
  default = 10
}

variable "scale_up_threshold" {
  type    = number
  default = 70
}

variable "scale_down_threshold" {
  type    = number
  default = 30
}

variable "health_check_grace_period_seconds" {
  description = "Seconds to let a task boot before failed ALB health checks count against it"
  type        = number
  default     = 180
}
