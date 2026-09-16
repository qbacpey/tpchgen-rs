#!/bin/bash
# Create a squashfs (.sqsh) file from a Docker image tarball
#
# This script runs inside a Docker container to create a squashfs file
# that can be used with Slurm/enroot, without needing enroot installed locally.
#
# Usage:
#   ./scripts/create-squashfs.sh [input.tar] [output.sqsh]
#
# Defaults:
#   input:  tpchgen-cli-arm64.tar
#   output: tpchgen-cli.sqsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

INPUT_TAR="${1:-tpchgen-cli-arm64.tar}"
OUTPUT_SQSH="${2:-tpchgen-cli.sqsh}"

cd "$PROJECT_ROOT"

if [[ ! -f "$INPUT_TAR" ]]; then
    echo "Error: Input file '$INPUT_TAR' not found"
    echo "Run ./scripts/build-container.sh first to create the Docker image tarball"
    exit 1
fi

echo "==> Creating squashfs from Docker image..."
echo "    Input:  $INPUT_TAR"
echo "    Output: $OUTPUT_SQSH"

docker run --rm -v "$PROJECT_ROOT:/work" -w /work ubuntu:22.04 bash -c '
set -e

INPUT_TAR="$1"
OUTPUT_SQSH="$2"

# Install dependencies quietly
apt-get update -qq
apt-get install -y -qq squashfs-tools jq > /dev/null 2>&1

echo "==> Extracting Docker image layers..."
mkdir -p /tmp/rootfs /tmp/image

# Extract the Docker tarball
cd /tmp/image
tar -xf "/work/$INPUT_TAR"

# Find and extract all layers
for layer in $(jq -r ".[0].Layers[]" manifest.json); do
    echo "    Extracting layer: $layer"
    tar -xf "$layer" -C /tmp/rootfs
done

echo "==> Creating squashfs (this may take a moment)..."
mksquashfs /tmp/rootfs "/work/$OUTPUT_SQSH" -noappend -all-root -quiet

echo "==> Done!"
' -- "$INPUT_TAR" "$OUTPUT_SQSH"

echo ""
echo "==> Squashfs created successfully!"
ls -lh "$OUTPUT_SQSH"
echo ""
echo "Next steps:"
echo "  1. Transfer to your cluster:"
echo "       scp $OUTPUT_SQSH user@cluster:/path/to/images/"
echo ""
echo "  2. Run with Slurm:"
echo "       srun --container-image=/path/to/$OUTPUT_SQSH \\"
echo "            --container-mounts=/scratch:/output \\"
echo "            tpchgen-cli parquet -s 10 --output-dir=/output"
