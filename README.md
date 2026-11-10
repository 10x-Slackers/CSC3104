# CSC3104 Cloud and Distributed Computing Project

> [!NOTE]
> WIP, remove this note when project is ready.

Privacy Preserving Detection of Cross Bank Money Mule Chains

---

## Project Scope

- [docs/](docs/README.md): requirements, architecture and per-role contracts

## Usage

```sh
uv sync --all-packages --locked                                   # install every app
uv run pytest                                                     # run all tests
uv run python -m scorer.baseline apps/dataset/fixtures/tiny B   # local-only detections for Bank B
deploy/k3d-up.sh && deploy/check-netpol.sh                        # local cluster, then policy check
deploy/replay.sh slice-6h --speed 60                              # replay apps/dataset/out/slice-6h into the cluster
```

- See each contract in `docs/contracts/` for app-specific commands.
- `deploy/k3d-up.sh` needs Docker, k3d, kubectl and Helm. The Dev Container doesn't include them, so run it on the host.

## Getting Started

### Prerequisites

- [Git](https://github.com/git-guides/install-git) (fully set-up)
- [Docker/Podman](https://docs.docker.com/engine/install/)
- [VS Code](https://code.visualstudio.com/download)
  - [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension

> [!WARNING]  
> Do not use GitHub Desktop! All interactions (files, git, runtime, etc.) should be done through the Dev Container within VS Code, except the k3d cluster (see Usage).

### Installation

1. Clone the repo

   ```sh
   git clone git@github.com:10x-Slackers/CSC3104.git
   ```

2. Open the repository in VS Code

   ```sh
   code CSC3104/
   ```

3. Click on the "Re-open in Dev Container" prompt
4. Install the pre-commit hooks

   ```sh
   pre-commit install
   ```

5. Start working!

## Developer Tooling

- Dev Containers
  - Standardised developer environment
- Pre-Commit
  - Run linting and formatting for all files during git commit
