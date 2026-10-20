# AWS permissions for pods, by EKS Pod Identity: each role trusts the Pod
# Identity agent, and an association binds it to one namespace and service
# account. Nothing in the cluster holds an access key, and the node role grants
# pods nothing. The AWS SDK v2 in both images reads Pod Identity credentials
# through its container credentials provider, with no extra module.

locals {
  pod_identity_trust = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
    }]
  })
}

# --- gate: the same grants as the ECS task role (modules/ecs/main.tf) --------

resource "aws_iam_role" "gate" {
  name               = "${local.name}-gate-pod"
  assume_role_policy = local.pod_identity_trust
}

resource "aws_iam_role_policy" "gate" {
  name = "gate"
  role = aws_iam_role.gate.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        # The quota store's transactions are authorised per item, by these.
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:ConditionCheckItem",
          "dynamodb:Query",
          "dynamodb:DescribeTable",
        ]
        Resource = [
          module.dynamodb.rate_limit_table_arn,
          module.dynamodb.idempotency_table_arn,
          module.dynamodb.token_quota_table_arn,
        ]
      },
      {
        Effect = "Allow"
        # DescribeStreamSummary is what /ready calls.
        Action   = ["kinesis:PutRecord", "kinesis:PutRecords", "kinesis:DescribeStream", "kinesis:DescribeStreamSummary"]
        Resource = [module.kinesis.stream_arn]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = [module.secrets.api_keys_secret_arn]
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
        Condition = {
          StringEquals = { "cloudwatch:namespace" = "RateLimiter/${var.environment}" }
        }
      },
    ]
  })
}

resource "aws_eks_pod_identity_association" "gate" {
  cluster_name    = aws_eks_cluster.main.name
  namespace       = local.namespace
  service_account = "gate"
  role_arn        = aws_iam_role.gate.arn
}

# --- usage aggregator: read the stream, write the usage table -----------------

resource "aws_iam_role" "aggregator" {
  name               = "${local.name}-aggregator-pod"
  assume_role_policy = local.pod_identity_trust
}

resource "aws_iam_role_policy" "aggregator" {
  name = "usage-aggregator"
  role = aws_iam_role.aggregator.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:ListShards",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:DescribeStreamSummary",
        ]
        Resource = [module.kinesis.stream_arn]
      },
      {
        Effect = "Allow"
        # A commit is one TransactWriteItems of UpdateItems (ADR-007).
        Action   = ["dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:Query", "dynamodb:DescribeTable"]
        Resource = [aws_dynamodb_table.usage.arn]
      },
    ]
  })
}

resource "aws_eks_pod_identity_association" "aggregator" {
  cluster_name    = aws_eks_cluster.main.name
  namespace       = local.namespace
  service_account = "gate-usage-aggregator"
  role_arn        = aws_iam_role.aggregator.arn
}

# --- AWS Load Balancer Controller ---------------------------------------------

resource "aws_iam_policy" "lb_controller" {
  name = "${local.name}-aws-load-balancer-controller"
  # Upstream's policy for v2.10.1, unchanged:
  # https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/v2.10.1/docs/install/iam_policy.json
  policy = file("${path.module}/policies/aws-load-balancer-controller.json")
}

resource "aws_iam_role" "lb_controller" {
  name               = "${local.name}-aws-load-balancer-controller"
  assume_role_policy = local.pod_identity_trust
}

resource "aws_iam_role_policy_attachment" "lb_controller" {
  role       = aws_iam_role.lb_controller.name
  policy_arn = aws_iam_policy.lb_controller.arn
}

resource "aws_eks_pod_identity_association" "lb_controller" {
  cluster_name    = aws_eks_cluster.main.name
  namespace       = "kube-system"
  service_account = "aws-load-balancer-controller"
  role_arn        = aws_iam_role.lb_controller.arn
}
