#!/usr/bin/env bash
set -euo pipefail

AWS_CLI_VERSION="2.37.6"
TERRAFORM_VERSION="1.16.4"
AWS_CLI_FINGERPRINT="FB5DB77FD5C118B80511ADA8A6310ACC4672475C"
HASHICORP_FINGERPRINT="798AEC654E5C15428C8E42EEAA16FCBCA621E701"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <tool-root>" >&2
  exit 2
fi
if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "install-compat-tools.sh supports Linux x86_64 only" >&2
  exit 2
fi
for command in curl gpg sha256sum unzip; do
  command -v "$command" >/dev/null || { echo "missing required command: $command" >&2; exit 2; }
done

TOOL_ROOT=$(mkdir -p "$1" && cd "$1" && pwd)
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT
GNUPGHOME="$TEMP_DIR/gnupg"
export GNUPGHOME
mkdir -m 700 "$GNUPGHOME"

verify_key() {
  local expected=$1
  local actual
  actual=$(gpg --batch --with-colons --fingerprint "$expected" | awk -F: '$1 == "fpr" { print $10; exit }')
  [[ "$actual" == "$expected" ]] || { echo "unexpected signing key fingerprint: $actual" >&2; exit 1; }
}

gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys "$AWS_CLI_FINGERPRINT"
verify_key "$AWS_CLI_FINGERPRINT"
curl --fail --silent --show-error --location \
  "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-${AWS_CLI_VERSION}.zip" \
  --output "$TEMP_DIR/awscliv2.zip"
curl --fail --silent --show-error --location \
  "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-${AWS_CLI_VERSION}.zip.sig" \
  --output "$TEMP_DIR/awscliv2.zip.sig"
gpg --batch --verify "$TEMP_DIR/awscliv2.zip.sig" "$TEMP_DIR/awscliv2.zip"
unzip -q "$TEMP_DIR/awscliv2.zip" -d "$TEMP_DIR/aws"
"$TEMP_DIR/aws/aws/install" --install-dir "$TOOL_ROOT/aws-cli" --bin-dir "$TOOL_ROOT/aws"

gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys "$HASHICORP_FINGERPRINT"
verify_key "$HASHICORP_FINGERPRINT"
TERRAFORM_BASE="https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}"
curl --fail --silent --show-error --location \
  "$TERRAFORM_BASE/terraform_${TERRAFORM_VERSION}_linux_amd64.zip" \
  --output "$TEMP_DIR/terraform.zip"
curl --fail --silent --show-error --location \
  "$TERRAFORM_BASE/terraform_${TERRAFORM_VERSION}_SHA256SUMS" \
  --output "$TEMP_DIR/terraform_SHA256SUMS"
curl --fail --silent --show-error --location \
  "$TERRAFORM_BASE/terraform_${TERRAFORM_VERSION}_SHA256SUMS.sig" \
  --output "$TEMP_DIR/terraform_SHA256SUMS.sig"
gpg --batch --verify "$TEMP_DIR/terraform_SHA256SUMS.sig" "$TEMP_DIR/terraform_SHA256SUMS"
(cd "$TEMP_DIR" && grep ' terraform_.*_linux_amd64.zip$' terraform_SHA256SUMS | sha256sum --check --status -)
mkdir -p "$TOOL_ROOT/terraform"
unzip -qo "$TEMP_DIR/terraform.zip" -d "$TOOL_ROOT/terraform"

"$TOOL_ROOT/aws/aws" --version
"$TOOL_ROOT/terraform/terraform" --version
