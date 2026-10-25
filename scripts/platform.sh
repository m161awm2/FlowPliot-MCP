#!/usr/bin/env bash
# pmcp platform deployment helper. Every step is explicit; nothing runs without naming it.
#   scripts/platform.sh certs        request ACM certs (us-east-1 + region) and print DNS validation records
#   scripts/platform.sh wait-certs   wait until both certificates are ISSUED
#   scripts/platform.sh image        create ECR repo (stack pmcp-ecr), build arm64 image, push
#   scripts/platform.sh platform     deploy stack pmcp-platform (needs certs ISSUED and an image)
#   scripts/platform.sh onboarding   upload infra/tenant-onboarding.yaml to the public bucket
#   scripts/platform.sh dns          print the CNAME records to create at the DNS provider
#   scripts/platform.sh teardown     delete platform + ecr stacks, certs, public bucket objects (asks first)
set -euo pipefail
cd "$(dirname "$0")/.."
REGION="${PMCP_REGION:-ap-southeast-2}"
DOMAIN="${PMCP_DOMAIN:-m161awm.kr}"
PUBLIC="mcp.${DOMAIN}"
ORIGIN="origin-mcp.${DOMAIN}"
ALERT_EMAIL="${PMCP_ALERT_EMAIL:?set PMCP_ALERT_EMAIL}"
ACCT="$(aws sts get-caller-identity --query Account --output text)"
STATE=".pmcp-deploy.env"; touch "$STATE"; source "$STATE"
save() { grep -v "^$1=" "$STATE" > "$STATE.tmp" || true; echo "$1=$2" >> "$STATE.tmp"; mv "$STATE.tmp" "$STATE"; }

cert_records() {  # $1 region $2 arn
  aws acm describe-certificate --region "$1" --certificate-arn "$2" \
    --query 'Certificate.DomainValidationOptions[].ResourceRecord.[Name,Type,Value]' --output text
}

case "${1:-}" in
  certs)
    if [ -z "${CF_CERT:-}" ]; then
      CF_CERT=$(aws acm request-certificate --region us-east-1 --domain-name "$PUBLIC" --validation-method DNS \
        --idempotency-token pmcpcf --tags Key=managed-by,Value=pipeline-mcp --query CertificateArn --output text)
      save CF_CERT "$CF_CERT"
    fi
    if [ -z "${ORIGIN_CERT:-}" ]; then
      ORIGIN_CERT=$(aws acm request-certificate --region "$REGION" --domain-name "$ORIGIN" --validation-method DNS \
        --idempotency-token pmcporigin --tags Key=managed-by,Value=pipeline-mcp --query CertificateArn --output text)
      save ORIGIN_CERT "$ORIGIN_CERT"
    fi
    sleep 5
    echo "Add these CNAME records at your DNS provider (host part is relative to ${DOMAIN}):"
    cert_records us-east-1 "$CF_CERT"; cert_records "$REGION" "$ORIGIN_CERT" ;;
  wait-certs)
    aws acm wait certificate-validated --region us-east-1 --certificate-arn "$CF_CERT"
    aws acm wait certificate-validated --region "$REGION" --certificate-arn "$ORIGIN_CERT"
    echo "certificates issued" ;;
  image)
    aws cloudformation deploy --region "$REGION" --stack-name pmcp-ecr --template-file infra/ecr.yaml \
      --tags managed-by=pipeline-mcp --no-fail-on-empty-changeset
    REPO=$(aws cloudformation describe-stacks --region "$REGION" --stack-name pmcp-ecr \
      --query "Stacks[0].Outputs[?OutputKey=='RepositoryUri'].OutputValue" --output text)
    TAG="$(date -u +%Y%m%d%H%M%S)"
    aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "${REPO%%/*}"
    docker build --platform linux/arm64 -t "$REPO:$TAG" .
    docker push "$REPO:$TAG"
    save IMAGE "$REPO:$TAG"; echo "pushed $REPO:$TAG" ;;
  platform)
    for pair in "us-east-1 $CF_CERT" "$REGION $ORIGIN_CERT"; do
      set -- $pair
      st=$(aws acm describe-certificate --region "$1" --certificate-arn "$2" --query Certificate.Status --output text)
      [ "$st" = ISSUED ] || { echo "certificate $2 is $st, not ISSUED; add the DNS validation records first" >&2; exit 2; }
    done
    if aws cloudformation describe-stacks --region "$REGION" --stack-name pmcp-platform \
        --query 'Stacks[0].StackStatus' --output text 2>/dev/null | grep -q '^ROLLBACK_COMPLETE$'; then
      aws cloudformation delete-stack --region "$REGION" --stack-name pmcp-platform
      aws cloudformation wait stack-delete-complete --region "$REGION" --stack-name pmcp-platform
    fi
    PL=$(aws ec2 describe-managed-prefix-lists --region "$REGION" \
      --filters Name=prefix-list-name,Values=com.amazonaws.global.cloudfront.origin-facing \
      --query 'PrefixLists[0].PrefixListId' --output text)
    aws cloudformation deploy --region "$REGION" --stack-name pmcp-platform --template-file infra/platform.yaml \
      --capabilities CAPABILITY_NAMED_IAM --tags managed-by=pipeline-mcp --no-fail-on-empty-changeset \
      --parameter-overrides ImageUri="$IMAGE" PublicHostname="$PUBLIC" OriginHostname="$ORIGIN" \
        CloudFrontCertificateArn="$CF_CERT" OriginCertificateArn="$ORIGIN_CERT" CloudFrontPrefixListId="$PL" \
        AlertEmail="$ALERT_EMAIL" ;;
  onboarding)
    aws s3 cp infra/tenant-onboarding.yaml "s3://pmcp-public-${ACCT}/onboarding/tenant-onboarding.yaml" \
      --region "$REGION" --content-type text/yaml ;;
  dns)
    out() { aws cloudformation describe-stacks --region "$REGION" --stack-name pmcp-platform \
      --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
    echo "CNAME  ${PUBLIC}  ->  $(out CloudFrontDomain)"
    echo "CNAME  ${ORIGIN}  ->  $(out AlbDns)" ;;
  teardown)
    read -r -p "Delete pmcp-platform, pmcp-ecr, certificates and onboarding objects in ${ACCT}/${REGION}? type yes: " a
    [ "$a" = yes ] || exit 1
    aws s3 rm "s3://pmcp-public-${ACCT}" --recursive --region "$REGION" || true
    aws cloudformation delete-stack --region "$REGION" --stack-name pmcp-platform
    aws cloudformation wait stack-delete-complete --region "$REGION" --stack-name pmcp-platform
    aws cloudformation delete-stack --region "$REGION" --stack-name pmcp-ecr
    [ -n "${CF_CERT:-}" ] && aws acm delete-certificate --region us-east-1 --certificate-arn "$CF_CERT" || true
    [ -n "${ORIGIN_CERT:-}" ] && aws acm delete-certificate --region "$REGION" --certificate-arn "$ORIGIN_CERT" || true
    echo "Retained on purpose: DynamoDB table pmcp-state (delete with: aws dynamodb delete-table --table-name pmcp-state)" ;;
  *) sed -n 2,10p "$0"; exit 1 ;;
esac
