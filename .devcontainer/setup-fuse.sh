#!/bin/bash
set -e

echo "=== FUSE setup check ==="

# Verify /dev/fuse is accessible.
if [ -c /dev/fuse ]; then
    echo "✓ /dev/fuse present"
else
    echo "✗ /dev/fuse not found — FUSE tests will be skipped"
    echo "  (Run the container with --device=/dev/fuse --cap-add=SYS_ADMIN)"
    exit 0
fi

# Verify current user can access /dev/fuse.
if [ -r /dev/fuse ] && [ -w /dev/fuse ]; then
    echo "✓ /dev/fuse readable+writable"
else
    echo "✗ /dev/fuse not accessible — check group membership"
    ls -la /dev/fuse
    groups
fi

# Verify user_allow_other is set.
if grep -q 'user_allow_other' /etc/fuse.conf 2>/dev/null; then
    echo "✓ user_allow_other enabled"
else
    echo "⚠ user_allow_other not set in /etc/fuse.conf"
fi

# Pre-download Go modules.
echo ""
echo "=== Downloading Go modules ==="
cd /workspaces/cvmfs-gc-optim/gc && go mod download

echo ""
echo "=== Ready ==="
echo "Run tests:  cd gc && go test ./fuse/ -v -count=1"
echo "FUSE E2E:   cd gc && go test ./fuse/ -run TestE2E -v -count=1"
echo "Build:      cd gc && go build ./..."
