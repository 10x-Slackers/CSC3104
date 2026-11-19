#!/usr/bin/env bash
# Builds the ingestor and summary Lambda zips into terraform/build/: the locked dependencies as wheels for the
# python3.12 runtime (x86-64 Amazon Linux 2023, glibc 2.34), plus the package's own source. Called by eks-up.sh before terraform apply.
set -euo pipefail
cd "$(dirname "$0")/terraform"
for app in ingestor summary; do
  rm -rf "build/$app" "build/$app.zip"
  uv export -q --package "$app" --no-dev --no-emit-workspace --no-hashes --no-header -o "build/$app.txt"
  uv pip install -q --target "build/$app" --python-platform x86_64-manylinux_2_28 --python-version 3.12 \
    --only-binary :all: -r "build/$app.txt"
  cp -r "../../apps/$app/src/$app" "build/$app/"
  (cd "build/$app" && zip -qr -X "../$app.zip" . -x '*__pycache__*')
done
