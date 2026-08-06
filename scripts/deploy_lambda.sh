#!/usr/bin/env bash
# Refreshes the source files inside the prebuilt Lambda package and ships it.
# Dependencies are NOT reinstalled here — rebuild build/lambda_package from pyproject.toml
# when they change; this script only carries src/ changes to AWS.
set -euo pipefail

PROFILE="${AWS_PROFILE:-amazon-books-etl-dev}"
REGION="${AWS_REGION:-eu-central-1}"
FUNCTION="amazon-books-scraper"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACKAGE="$ROOT/build/lambda_package"

[ -d "$PACKAGE/pydantic" ] || {
    echo "error: $PACKAGE looks unbuilt (no dependencies) — rebuild it first" >&2
    exit 1
}

cp "$ROOT"/src/*.py "$ROOT"/src/config.yaml "$PACKAGE/"

# Built with Python's zipfile rather than the `zip` binary — the latter is not installed on
# every machine (it is missing on this WSL image), and a deploy script must not need extra apt.
# Atomic write: archive to a temp path, rename on success — an interrupted build must never
# leave a truncated zip that would deploy a broken function.
tmp_zip="$(mktemp -u "${TMPDIR:-/tmp}/lambda-XXXXXX.zip")"
zip_path="$ROOT/build/lambda_package.zip"
python3 - "$PACKAGE" "$tmp_zip" <<'PY'
import pathlib, sys, zipfile

package, target = pathlib.Path(sys.argv[1]), sys.argv[2]
with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(package.rglob("*")):
        if path.is_dir() or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        archive.write(path, path.relative_to(package))
PY
mv "$tmp_zip" "$zip_path"

aws lambda update-function-code \
    --function-name "$FUNCTION" \
    --zip-file "fileb://$zip_path" \
    --profile "$PROFILE" --region "$REGION" \
    --query '{Function:FunctionName,Size:CodeSize,Modified:LastModified}' --output table

aws lambda wait function-updated --function-name "$FUNCTION" --profile "$PROFILE" --region "$REGION"
echo "Deployed. Verify with: aws lambda invoke --function-name $FUNCTION ..."
