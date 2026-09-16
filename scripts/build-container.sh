#!/bin/bash
# Build a container image for ARM (aarch64) systems
#
# This script builds the tpchgen-cli container for linux/arm64 and exports
# it as a tarball that can be converted to a squash file for Slurm/enroot.
#
# Usage:
#   ./scripts/build-container.sh
#
# Output:
#   tpchgen-cli-arm64.tar - Docker image tarball
#
# To convert to squash file on the cluster:
#   enroot import docker-archive://tpchgen-cli-arm64.tar
#
# To run with Slurm:
#   srun --container-image=/path/to/tpchgen-cli.sqsh tpchgen-cli parquet -s 10 --output-dir=/output

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

IMAGE_NAME="tpchgen-cli"
IMAGE_TAG="latest"
OUTPUT_FILE="tpchgen-cli-arm64.tar"

cd "$PROJECT_ROOT"

echo "==> Building ${IMAGE_NAME}:${IMAGE_TAG}..."

# Check if buildx is available for cross-platform builds
if docker buildx version &>/dev/null; then
    echo "    Using buildx for build"
    
    # Create builder if needed
    if ! docker buildx inspect arm-builder &>/dev/null; then
        docker buildx create --name arm-builder --use
    else
        docker buildx use arm-builder
    fi
    
    docker buildx build \
        --platform linux/arm64 \
        --tag "${IMAGE_NAME}:${IMAGE_TAG}" \
        --load \
        .
else
    echo "    Using standard docker build"
    # Standard docker build (works when host matches target arch)
    docker build \
        --tag "${IMAGE_NAME}:${IMAGE_TAG}" \
        .
fi

echo "==> Exporting image to ${OUTPUT_FILE}..."
docker save "${IMAGE_NAME}:${IMAGE_TAG}" -o "$OUTPUT_FILE"

IMAGE_SIZE=$(ls -lh "$OUTPUT_FILE" | awk '{print $5}')
echo ""
echo "==> Done!"
echo "    Image saved to: ${PROJECT_ROOT}/${OUTPUT_FILE}"
echo "    Size: ${IMAGE_SIZE}"
echo ""
echo "Next steps:"
echo "  1. Transfer the tarball to your cluster:"
echo "       scp ${OUTPUT_FILE} user@cluster:/path/to/images/"
echo ""
echo "  2. On the cluster, convert to squash file:"
echo "       enroot import docker-archive://${OUTPUT_FILE}"
echo ""
echo "  3. Run with Slurm:"
echo "       srun --container-image=/path/to/tpchgen-cli+latest.sqsh \\"
echo "            --container-mounts=/scratch:/output \\"
echo "            tpchgen-cli parquet -s 10 --output-dir=/output"
