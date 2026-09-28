# shellcheck shell=bash
# lib-terraform.sh - sourced by bootstrap.sh, deploy-demo.sh and teardown-demo.sh.
#
# Terraform state lives in S3 with native lockfile locking (use_lockfile),
# which needs Terraform 1.10 or later; 1.11 is the first release where it is
# not experimental. backend.hcl is written by bootstrap.sh and names the
# bucket; each root adds its own state key.

BACKEND_HCL="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/terraform/backend.hcl"

require_terraform() {
  command -v terraform >/dev/null 2>&1 || {
    echo "Error: terraform not found in PATH." >&2; exit 1; }
  local version major minor
  version=$(terraform version -json 2>/dev/null \
    | sed -nE 's/.*"terraform_version": *"([0-9]+\.[0-9]+)\..*/\1/p')
  major=${version%%.*}; minor=${version#*.}
  if [ -z "$version" ] || [ "$major" -lt 1 ] || { [ "$major" -eq 1 ] && [ "$minor" -lt 10 ]; }; then
    echo "Error: Terraform 1.10 or later is required for S3 state locking; found $(terraform version | head -1)." >&2
    exit 1
  fi
}

# Terraform's AWS provider cannot read every credential source the AWS CLI can:
# an `aws login` session is one, and the first deploy with these scripts
# failed on it with "No valid credential sources found" while the CLI worked.
# The CLI hands over its resolved, temporary credentials as environment
# variables, which every provider version reads.
use_cli_credentials() {
  [ -n "${AWS_ACCESS_KEY_ID:-}" ] && return 0
  local creds
  creds=$(aws configure export-credentials --format env 2>/dev/null) || {
    echo "Error: the AWS CLI has no usable credentials. Run 'aws login' or configure a profile first." >&2
    exit 1; }
  eval "$creds"
}

require_backend() {
  [ -f "$BACKEND_HCL" ] || {
    echo "Error: $BACKEND_HCL is missing. Create the state bucket first:" >&2
    echo "   ./scripts/bootstrap.sh" >&2
    exit 1; }
}

# tf_init <state key>: initialise the current directory against the S3 state.
tf_init() {
  terraform init -input=false -reconfigure \
    -backend-config="$BACKEND_HCL" -backend-config="key=$1"
}
