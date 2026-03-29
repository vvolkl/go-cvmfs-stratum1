FROM golang:1.25-bookworm

# FUSE3 + build tools.
RUN apt-get update && apt-get install -y --no-install-recommends \
        fuse3 libfuse3-dev \
        git make curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Allow non-root FUSE mounts.
RUN echo 'user_allow_other' >> /etc/fuse.conf

# Create non-root dev user in the fuse group.
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000
RUN groupadd -f fuse \
    && (getent group ${USER_GID} || groupadd --gid ${USER_GID} ${USERNAME}) \
    && useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME} || true \
    && usermod -aG fuse ${USERNAME}

# Pre-install Go tools for VS Code.
RUN go install golang.org/x/tools/gopls@latest \
    && go install github.com/go-delve/delve/cmd/dlv@latest \
    && go install honnef.co/go/tools/cmd/staticcheck@latest

USER ${USERNAME}
WORKDIR /workspaces/cvmfs-gc-optim
