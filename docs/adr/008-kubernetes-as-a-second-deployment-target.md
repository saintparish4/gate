# ADR-008: Kubernetes (EKS) as a Second Deployment Target

**Context:** Gate is deployed one way today: Terraform's ECS Fargate service behind an ALB (`terraform/main.tf`), used by the demo and by every real-AWS correctness run. An operator who runs a Kubernetes platform cannot use that. They need a chart that installs into their cluster, takes AWS permissions the way their cluster grants them, and reports into the Prometheus and Grafana they already have.

## Decision

Gate ships a Helm chart (`deploy/helm/gate`) and a Terraform root that builds an EKS cluster for it (`terraform/eks`). ECS stays: it is the demo, it is what the README's AWS evidence ran on, and nothing about it changes.

- **The chart installs** the service and the usage aggregator (ADR-007), and optionally an ALB Ingress, ServiceMonitors, the SLO PrometheusRule, and the Grafana dashboards.
- **The Terraform root builds** the VPC (the shared networking module), the cluster and one managed node group, the DynamoDB tables, the Kinesis stream, the API-keys secret, the usage table, and one IAM role per pod.
- **`scripts/deploy-eks.sh`** applies the root, writes fresh API keys into the secret before any pod starts, installs the AWS Load Balancer Controller and kube-prometheus-stack, and installs the chart. `scripts/teardown-eks.sh` removes it all, the ALB first.

## How It Keeps the Promises ECS Keeps

The ECS root encodes several hard-won rules. Each has an equivalent here, enforced by the chart or the root, not by a README:

| Rule | ECS | EKS |
| --- | --- | --- |
| No plaintext ALB open to the internet | Terraform refuses `0.0.0.0/0` without a certificate | the chart's Ingress template fails to render on the same condition |
| Keys come from Secrets Manager, never the public built-ins | `SECRETS_MANAGER_ENABLED = "true"` | the chart fails to render without it |
| Degradation posture pinned, never inherited | `DEGRADATION_MODE` set | the same value in `gate.env` |
| One env, everywhere | `check-env-drift.sh` compares compose and Terraform | it also compares the chart's `gate.env` with Terraform's |
| Images by immutable commit tag | ECR `IMMUTABLE` | the chart refuses an empty tag or `latest` |
| Slow start for a cold JVM | target group `slow_start` | the same attribute on the Ingress, plus a startup probe |
| Client address behind the ALB | `AUTH_TRUST_FORWARDED_FOR = "true"` | the same, with `target-type: ip` |

## Pod Identity, Not IRSA

Pods get AWS credentials from **EKS Pod Identity**: a role per service account, trusted by `pods.eks.amazonaws.com`, bound by an association in Terraform. IRSA would have worked too; Pod Identity was chosen because:

- **No OIDC provider and no role ARN in the chart.** The association lives in Terraform, keyed by namespace and service account name, so the chart carries no account-specific annotation.
- **No extra SDK module.** The AWS SDK v2 reads Pod Identity credentials through its container credentials provider, already on the classpath. IRSA's web-identity provider needs `software.amazon.awssdk:sts`, which the service does not depend on; without it the SDK would silently fall through to the node's role.
- **The node role grants pods nothing**, so a pod whose association is missing fails to reach AWS rather than borrowing the node's permissions.

## What's Sacrificed

- **Two deployments to keep in step.** The drift check covers the environment; it does not cover IAM, which is written twice (`modules/ecs` and `terraform/eks/pod-identity.tf`). A permission added to one must be added to the other by hand.
- **Not run on AWS yet.** The root validates without credentials, and the chart is linted, schema-checked, and rehearsed on a local kind cluster against LocalStack (`scripts/kind-rehearsal.sh`). The first apply on a real account, and the first correctness run across EKS replicas, are still to come. Until then the README claims ECS only.
- **Cost.** A cluster is about $13 a day while up (control plane, three nodes, NAT gateways, ALB), several times the ECS demo. It is a deploy-test-tear-down environment, not one to leave running.
- **Upgrades are the operator's.** The root pins the Kubernetes version and lets EKS pick matching add-on versions; the deploy script pins both Helm charts it installs. Moving any of them is a deliberate change.

## References

- `deploy/helm/gate`, `deploy/README.md`
- `terraform/eks`
- `scripts/deploy-eks.sh`, `scripts/teardown-eks.sh`, `scripts/kind-rehearsal.sh`
