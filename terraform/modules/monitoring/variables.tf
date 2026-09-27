variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "ecs_cluster_name" {
  type = string
}

variable "ecs_service_name" {
  type = string
}

variable "alb_arn_suffix" {
  type = string
}

variable "target_group_arn_suffix" {
  type = string
}

variable "min_healthy_tasks" {
  description = "Alarm when fewer tasks than this are healthy"
  type        = number
}

variable "alarm_sns_topic_arn" {
  type    = string
  default = ""
}
