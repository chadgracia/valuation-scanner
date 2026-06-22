#!/usr/bin/env bash
#
# deploy.sh — package and deploy the valuation-scanner Lambda.
#
# Zips lambda_function.py (boto3 ships in the Lambda runtime, so no vendored
# deps are needed) and pushes the code to AWS Lambda. On first run, pass
# CREATE=true with a ROLE_ARN to create the function; subsequent runs just
# update the code.
#
# Usage:
#   ./deploy.sh                       # update code on the existing function
#   CREATE=true ROLE_ARN=arn:... ./deploy.sh   # create the function first time
#
# Overridable via env:
#   FUNCTION_NAME  (default: valuation-scanner)
#   REGION         (default: us-east-1)   # Bedrock + SES live here
#   RUNTIME        (default: python3.12)
#   TIMEOUT        (default: 900)         # 15 min, the Lambda max
#   MEMORY         (default: 512)
#
set -euo pipefail

FUNCTION_NAME="${FUNCTION_NAME:-valuation-scanner}"
REGION="${REGION:-us-east-1}"
RUNTIME="${RUNTIME:-python3.12}"
TIMEOUT="${TIMEOUT:-900}"
MEMORY="${MEMORY:-512}"
HANDLER="lambda_function.lambda_handler"

cd "$(dirname "$0")"

ZIP="$(mktemp -t valuation-scanner.XXXXXX.zip)"
trap 'rm -f "$ZIP"' EXIT

echo "Packaging lambda_function.py -> $ZIP"
zip -j -q "$ZIP" lambda_function.py

if [ "${CREATE:-false}" = "true" ]; then
    if [ -z "${ROLE_ARN:-}" ]; then
        echo "ERROR: CREATE=true requires ROLE_ARN=arn:aws:iam::...:role/..." >&2
        exit 1
    fi
    echo "Creating function $FUNCTION_NAME in $REGION"
    aws lambda create-function \
        --region "$REGION" \
        --function-name "$FUNCTION_NAME" \
        --runtime "$RUNTIME" \
        --role "$ROLE_ARN" \
        --handler "$HANDLER" \
        --timeout "$TIMEOUT" \
        --memory-size "$MEMORY" \
        --environment "Variables={DRY_RUN=true,BRAVE_API_KEY=${BRAVE_API_KEY:-}}" \
        --zip-file "fileb://$ZIP"
else
    echo "Updating code for $FUNCTION_NAME in $REGION"
    aws lambda update-function-code \
        --region "$REGION" \
        --function-name "$FUNCTION_NAME" \
        --zip-file "fileb://$ZIP" \
        --publish
fi

echo "Done."
