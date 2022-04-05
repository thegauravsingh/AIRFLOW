# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.205.2/containers/ubuntu/.devcontainer/base.Dockerfile

# [Choice] Ubuntu version (use hirsuite or bionic on local arm64/Apple Silicon): hirsute, focal, bionic
ARG VARIANT="hirsute"
FROM mcr.microsoft.com/vscode/devcontainers/base:0-${VARIANT}

#[Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends iputils-ping

RUN useradd -rm -d /home/ssh_user -s /bin/bash ssh_user && \
    echo ssh_user:ssh_user1234 | chpasswd

RUN mkdir /home/ssh_user/.ssh && \
    chmod 700 /home/ssh_user/.ssh

COPY id_rsa_shared.pub /home/ssh_user/.ssh/authorized_keys

RUN chown ssh_user:ssh_user -R /home/ssh_user/.ssh && \
    chmod 600 /home/ssh_user/.ssh/authorized_keys

