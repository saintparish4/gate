# The cluster. Access is by EKS access entries (authentication_mode = API), not
# the aws-auth ConfigMap; whoever applies this root is the first admin.

resource "aws_kms_key" "eks" {
  description             = "${local.name} EKS secrets encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_iam_role" "cluster" {
  name = "${local.name}-cluster"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_cloudwatch_log_group" "cluster" {
  # The name EKS writes control-plane logs to; created here so it has a
  # retention instead of keeping logs forever.
  name              = "/aws/eks/${local.name}/cluster"
  retention_in_days = 7
}

resource "aws_eks_cluster" "main" {
  name     = local.name
  version  = var.kubernetes_version
  role_arn = aws_iam_role.cluster.arn

  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = true
  }

  vpc_config {
    subnet_ids              = concat(module.networking.private_subnet_ids, module.networking.public_subnet_ids)
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.api_access_cidrs
  }

  encryption_config {
    resources = ["secrets"]
    provider {
      key_arn = aws_kms_key.eks.arn
    }
  }

  enabled_cluster_log_types = ["api", "audit", "authenticator"]

  depends_on = [
    aws_iam_role_policy_attachment.cluster,
    aws_cloudwatch_log_group.cluster,
  ]
}

resource "aws_iam_role" "node" {
  name = "${local.name}-node"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# The node role has only what a node needs. Pods get their AWS permissions
# from Pod Identity (pod-identity.tf); none fall back to these.
resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ])
  role       = aws_iam_role.node.name
  policy_arn = each.value
}

resource "aws_eks_node_group" "main" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${local.name}-default"
  node_role_arn   = aws_iam_role.node.arn
  # Private subnets only: nodes reach AWS through the NAT gateways.
  subnet_ids     = module.networking.private_subnet_ids
  instance_types = var.node_instance_types
  ami_type       = "AL2023_x86_64_STANDARD"

  scaling_config {
    desired_size = var.node_desired_size
    min_size     = var.node_min_size
    max_size     = var.node_max_size
  }

  update_config {
    max_unavailable = 1
  }

  # The autoscaler (if one is added) owns desired_size after the first apply.
  lifecycle {
    ignore_changes = [scaling_config[0].desired_size]
  }

  depends_on = [aws_iam_role_policy_attachment.node]
}

# Versions are left to EKS's default for the cluster version, so an upgrade of
# kubernetes_version brings matching add-ons.
resource "aws_eks_addon" "core" {
  for_each = toset(["vpc-cni", "kube-proxy", "coredns", "eks-pod-identity-agent"])

  cluster_name                = aws_eks_cluster.main.name
  addon_name                  = each.value
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  # coredns needs nodes to schedule on.
  depends_on = [aws_eks_node_group.main]
}
