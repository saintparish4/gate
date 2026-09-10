# Deploy runbook — from a clean laptop to numbers in the README

The one caveat in Gate's [Status](../README.md#status) is that the Terraform
stack has never been applied, so every performance figure in this repository is
a LocalStack figure. This runbook closes that. It is written to be followed
once, produce real numbers, and then destroy everything.

Budget about **90 minutes** and **under $5**. The stack is the demo profile:
one ECS task, no autoscaling, no Firehose.

---

## Before you start

| Requirement | Check |
|---|---|
| AWS account with admin-ish rights | `aws sts get-caller-identity` |
| Terraform ≥ 1.5 | `terraform version` |
| Docker | `docker info` |
| k6 (for the load test) | `k6 version` |

Set a region once and keep it consistent — the load-test script defaults to
`us-east-1` when it looks up the load balancer:

```bash
export AWS_REGION=us-east-1
```

---

## 1. Push the image

```bash
./scripts/push-image.sh
export ECR_IMAGE=$(cat .ecr-image)
```

Builds `--platform linux/amd64` on purpose. Fargate is amd64; an arm64 image
pushes without complaint and then crash-loops with an exec format error that
only appears in CloudWatch.

## 2. Apply

```bash
./scripts/deploy-demo.sh
```

Deploys and then polls `/health` for up to ten minutes. First boot is slow:
ECS pulls the image, the ALB registers the target, and the health check needs
two consecutive passes.

**If it times out**, the stack exists and something inside it is unhealthy —
do not re-run the deploy, look at the task:

```bash
aws ecs list-tasks --cluster gate-demo
aws logs tail /ecs/gate-demo --follow --since 10m
```

The three failures worth expecting, in order of likelihood: the task role
cannot reach DynamoDB (IAM), the container cannot resolve Kinesis (VPC
endpoints or NAT), or the image is the wrong architecture (step 1).

## 3. Confirm it is actually enforcing

Before measuring anything, prove the thing under test works. A load test
against a service that returns 200 to everything produces beautiful numbers.

```bash
ALB=$(cd terraform && terraform output -raw load_balancer_dns)

curl -s "http://$ALB/health"

# Burst past the limit and confirm you get 429s and the headers.
for i in $(seq 1 120); do
  curl -s -o /dev/null -w '%{http_code} ' \
    -H 'X-Api-Key: runbook-key' "http://$ALB/v1/ratelimit/check"
done; echo
```

You want a run of `200`s followed by `429`s, not all `200`s. If every request
succeeds, the limiter is not in the path and nothing below is worth recording.

## 4. Load test

```bash
./scripts/load-test.sh demo baseline
```

Results land in `./test-results/<timestamp>/`. Run **baseline** first. Only run
`stress` once baseline is clean — otherwise a bad number has two possible
causes and you cannot tell which.

Record from the k6 summary:

- `http_req_duration` p50 / p95 / p99
- `http_reqs` rate (requests/s sustained)
- the 429 share — this is the point, not an error
- `dropped_kinesis_events` from `/metrics`, which is the back-pressure path
  doing its job under load

## 5. Capture the invariant under real contention

This is the figure that distinguishes Gate from a rate limiter that merely
looks fast: **over-issue count at high contention**. Same key, many concurrent
callers, one shared DynamoDB row.

```bash
ALB=$(cd terraform && terraform output -raw load_balancer_dns)
LIMIT=100   # whatever the configured capacity for the key is

seq 1 500 | xargs -P 50 -I{} \
  curl -s -o /dev/null -w '%{http_code}\n' \
    -H 'X-Api-Key: contention-key' "http://$ALB/v1/ratelimit/check" \
  | sort | uniq -c
```

Allowed (`200`) must be **≤ LIMIT**. Under-issuing at the tail is the designed
behaviour when OCC retries are exhausted; over-issuing even once means the
token-bucket invariant does not hold on real DynamoDB, which is a finding worth
far more than a latency table.

## 6. Tear down — do this today

```bash
./scripts/teardown-demo.sh
```

Then verify, because a partial destroy still bills:

```bash
aws ecs list-clusters
aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerName'
aws dynamodb list-tables
aws kinesis list-streams
```

An ALB left running is roughly $16/month on its own. A forgotten NAT gateway
is more.

## 7. Put the numbers in the README

Fill in the table under [Measured on AWS](../README.md#measured-on-aws) and
state the instance profile and date beside it, the same way the LocalStack
figures are qualified. Then delete the "never been applied" sentence from
Status — it will no longer be true.

---

## Cost notes

| Resource | Demo profile | Roughly |
|---|---|---|
| ECS Fargate | 1 task, 256 CPU / 512 MB | ~$0.02/hr |
| ALB | 1 | ~$0.023/hr + LCU |
| NAT gateway | 1 (if the VPC module creates one) | ~$0.045/hr + data |
| DynamoDB | `PAY_PER_REQUEST` | pennies at this volume |
| Kinesis | 1 shard | ~$0.015/hr |

Call it **$0.10–0.15/hour**, so a 90-minute session is well under a dollar.
The risk is not the hourly rate, it is leaving it up. Tear it down the same day.
