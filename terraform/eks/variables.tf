variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "gate"
}

variable "environment" {
  description = "Names every resource and the secret path (<project>/<environment>/api-keys). Kept apart from the ECS demo's, so the two never share a table."
  type        = string
  default     = "eks"
}

variable "kubernetes_version" {
  type    = string
  default = "1.31"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "availability_zones" {
  description = "At least two: an ALB needs subnets in two zones."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "public_subnet_cidrs" {
  type    = list(string)
  default = ["10.20.101.0/24", "10.20.102.0/24"]
}

variable "private_subnet_cidrs" {
  type    = list(string)
  default = ["10.20.1.0/24", "10.20.2.0/24"]
}

variable "api_access_cidrs" {
  description = "Who may reach the cluster's public API endpoint. No default: deploy-eks.sh sets the deployer's address."
  type        = list(string)

  validation {
    condition     = length(var.api_access_cidrs) > 0 && !contains(var.api_access_cidrs, "0.0.0.0/0")
    error_message = "api_access_cidrs must name specific addresses, not 0.0.0.0/0."
  }
}

variable "node_instance_types" {
  description = "A gate replica requests 1 vCPU and 2 GiB, as the ECS task does. m6i.large (2 vCPU, 8 GiB) fits one replica plus the system pods and the aggregator."
  type        = list(string)
  default     = ["m6i.large"]
}

variable "node_desired_size" {
  type    = number
  default = 3
}

variable "node_min_size" {
  type    = number
  default = 2
}

variable "node_max_size" {
  type    = number
  default = 4
}

variable "kinesis_shard_count" {
  type    = number
  default = 1
}

variable "kinesis_retention_hours" {
  description = "The usage aggregator can be down this long before records are lost to usage (ADR-007)."
  type        = number
  default     = 24
}

variable "usage_retention_days" {
  type    = number
  default = 90
}

variable "gate_image" {
  description = "The service image, as publish-image.sh prints it (repository:tag)."
  type        = string
  default     = ""
}

variable "aggregator_image" {
  description = "The aggregator image, as publish-image.sh prints it."
  type        = string
  default     = ""
}

variable "certificate_arn" {
  description = "ACM certificate for the ALB. Without one the ALB serves HTTP to alb_ingress_cidrs only."
  type        = string
  default     = ""
}

variable "alb_ingress_cidrs" {
  description = "Who may reach the ALB. deploy-eks.sh sets the deployer's address when there is no certificate."
  type        = list(string)
  default     = []
}
