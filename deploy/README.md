# Deploying Gate on Kubernetes

The Helm chart in [`helm/gate`](helm/gate) installs the service and the usage aggregator
([ADR-007](../docs/adr/007-usage-aggregation-from-the-event-stream.md)).
[`terraform/eks`](../terraform/eks) builds an EKS cluster for it, with all the AWS state it
needs. [ADR-008](../docs/adr/008-kubernetes-as-a-second-deployment-target.md) explains why
both this and the ECS deployment exist.

**Status:** prepared, not yet applied on AWS. The Terraform validates without credentials. The
chart is linted and schema-checked (`make helm-check`), and it has been rehearsed on a local
kind cluster against LocalStack, where the correctness invariants passed across its replicas
(`scripts/kind-rehearsal.sh`). IAM, Pod Identity, and the ALB only exist on AWS, so they are
untested until the first real deploy.

## What gets created

| Piece | Where | Notes |
| --- | --- | --- |
| VPC, two public and two private subnets, NAT | `modules/networking` (shared with ECS) | subnets tagged for the AWS Load Balancer Controller |
| EKS 1.31, one managed node group | `terraform/eks/eks.tf` | 3 × m6i.large in private subnets; API endpoint open to your address only; secrets encrypted with KMS; control-plane logs kept 7 days |
| DynamoDB tables, Kinesis stream, API-keys secret | the shared modules | environment `eks`, so nothing is shared with the ECS demo |
| `gate-eks-usage` table | `terraform/eks/main.tf` | the aggregator's sums and checkpoints; TTL and point-in-time recovery on |
| One IAM role per pod, by EKS Pod Identity | `terraform/eks/pod-identity.tf` | `gate`, `gate-usage-aggregator`, and the load balancer controller; the node role grants pods nothing |
| AWS Load Balancer Controller 2.10.1 | `scripts/deploy-eks.sh` | turns the chart's Ingress into an ALB |
| kube-prometheus-stack 65.8.1 | `scripts/deploy-eks.sh` | Prometheus with 30 days of retention, Grafana, Alertmanager |
| The gate chart | `deploy/helm/gate` | 2-6 replicas (HPA on CPU), the aggregator, ServiceMonitors, the SLO rules, both dashboards |

## Deploy

You need the AWS CLI, signed in (`aws login`, profile `gate`), Terraform 1.10 or later,
`kubectl`, Helm 3, and Docker.

```bash
./scripts/bootstrap.sh     # once per account, and again after this change: it adds the aggregator's ECR repository
export GATE_IMAGE=$(./scripts/publish-image.sh)
export AGGREGATOR_IMAGE=$(ECR_REPO_NAME=gate-usage-aggregator DOCKERFILE=aggregator.Dockerfile ./scripts/publish-image.sh)
./scripts/deploy-eks.sh    # 20-25 minutes, most of it the cluster
```

Without `CERTIFICATE_ARN`, the ALB serves plain HTTP to your current public address only. The
chart refuses to render a public plaintext ALB, which is the same rule Terraform applies to
the ECS ALB. With `CERTIFICATE_ARN` set to an ACM certificate, the ALB serves HTTPS and
redirects port 80. If your machine can leave through two public addresses (see `CLAUDE.md`),
set `ALB_INGRESS_CIDR` and `API_ACCESS_CIDR` yourself.

The script writes fresh random API keys into the secret before any pod starts, and saves them
to `.eks-keys.env` (gitignored, mode 600). Then:

```bash
source .eks-keys.env && make APP_URL=http://<alb-host> correctness    # invariants A-D across the replicas
kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80
```

Grafana has both dashboards, *Gate — SLOs* and *Gate — Rate Limiting & Quotas*. The SLO
objectives and alerts are in [`docs/SLO.md`](../docs/SLO.md). Alertmanager has no receivers
until you configure some.

## Cost and teardown

While it is up, the deploy costs roughly **$0.55 an hour, about $13 a day**: the control
plane is $0.10/h, three m6i.large nodes about $0.29/h, two NAT gateways about $0.09/h, and the
ALB, on-demand DynamoDB, and one Kinesis shard make up the rest. These are us-east-1 list
prices, not a bill.

```bash
./scripts/teardown-eks.sh
```

The teardown uninstalls the chart first and waits for the controller to delete the ALB.
Destroying the cluster first would orphan the ALB, and its security groups would then block
the VPC's deletion. The ECR images stay, because `terraform/bootstrap` owns them.

## Rehearse locally

```bash
bash scripts/kind-rehearsal.sh          # kind, LocalStack in the cluster, the chart, then make correctness
KEEP=0 bash scripts/kind-rehearsal.sh   # and delete the cluster afterwards
```

It builds the three images, runs a three-node kind cluster, writes random keys into
LocalStack's Secrets Manager, installs the chart with
[`ci/kind-values.yaml`](helm/gate/ci/kind-values.yaml) (two fixed replicas behind a NodePort
on `localhost:18080`), runs the invariants, and prints each replica's request count.

## Values worth knowing

| Value | Default | Why |
| --- | --- | --- |
| `gate.env.*` | what Terraform sets on ECS | `scripts/check-env-drift.sh` fails CI if the chart and Terraform diverge |
| `gate.image.tag` | none | an immutable commit tag; the chart refuses an empty tag or `latest` |
| `gate.resources` | 1 vCPU and 2 GiB requested, memory limit only | the ECS task's size; one replica is CPU-bound at about 450 decisions a second |
| `gate.preStopSleepSeconds` | 15 | the pod keeps serving while the ALB deregisters it |
| `aggregator.*` | one replica, `Recreate` | a second reader stays correct but wastes writes on fenced commits |
| `ingress.slowStartSeconds` | 60 | as on ECS; it only applies while another target is healthy |
| `metrics.serviceMonitor.adminKeySecret` | `gate-metrics-key` / `api-key` | `/metrics` needs an AdminMetrics key; the deploy script creates the Secret |
