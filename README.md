# CSC3104 Cloud and Distributed Computing Project

> [!NOTE]
> WIP, remove this note when project is ready.

Privacy Preserving Detection of Cross Bank Money Mule Chains

---

## Project Scope

Three simulated banks (A, B and C) trace money mule chains across banks without sharing customer records. Each bank keeps its payments and scoring local; peers exchange only transfer tokens and signed link claims.

- [IBM Transactions for Anti Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) (Altman et al., NeurIPS 2023)
  - Synthetic base data for all three banks: `HI-Small_Trans.csv`, version 8
- [Data contract](docs/data-contract.md)
  - Bank split, event and label formats, planted scenarios, workloads and expected results

## Role 1: Data and Replay (`macc-role1`)

This branch prepares the Altman dataset for Banks A, B and C, with shared payment IDs, labelled planted chains and a replay tool with a speed setting. Kafka, ingestion and scoring belong to Role 2: replay prints each bank's events for that bank's ingestor.

| File | Purpose |
|---|---|
| `apps/dataset/prepare.py` | Downloads `HI-Small_Trans.csv` from Kaggle and builds a workload: `bank_A.csv`, `bank_B.csv`, `bank_C.csv` (each bank's own events), `labels.csv` (evaluation only) and `workload.json` |
| `apps/dataset/replay.py` | Prints one bank's events as JSON lines in time order, paced by `--speed` |
| `apps/dataset/test_prepare.py` | Tests the bank split and the planted chains |
| `apps/dataset/fixtures/tiny/` | 20 hand-written transfers covering every scenario, with their bank files and labels |
| `apps/dataset/pyproject.toml` | The `dataset` workspace member and its dependencies |
| `docs/data-contract.md` | The agreed formats and expected results for Roles 2 to 6 |

Each transfer becomes two events with the same `payment_id`, one in the sender's bank file and one in the receiver's:

```text
bank_A.csv: p06,2022-09-05T09:10:00Z,A,out,A-m0,B,10000.0,Wire
bank_B.csv: p06,2022-09-05T09:10:00Z,B,in,B-m1,A,10000.0,Wire
```

## Usage

### apps/dataset

```sh
uv run apps/dataset/prepare.py apps/dataset/out/dev-small --start 2022-09-05 --hours 6 --seed 1
uv run apps/dataset/replay.py apps/dataset/out/dev-small A --speed 60 --start-at <epoch> | <ingestor>
```

- `prepare.py` downloads the dataset on its first run (about 476 MB, no Kaggle login, cached in `~/.cache/kagglehub`) and writes a workload to the given directory; `apps/dataset/out/` is gitignored. See the [data contract](docs/data-contract.md#workloads) for the dev and eval workloads
- `replay.py` runs once per bank with the same `--start-at` so the banks stay in step. `--speed 60` plays one simulated minute per second; `--speed 0` sends as fast as possible

## Getting Started

### Prerequisites

- [Git](https://github.com/git-guides/install-git) (fully set-up)
- [Docker/Podman](https://docs.docker.com/engine/install/)
- [VS Code](https://code.visualstudio.com/download)
  - [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension

> [!WARNING]  
> Do not use GitHub Desktop! All interactions (files, git, runtime, etc.) should be done through the Dev Container within VS Code.

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

5. Install the Python workspace

   ```sh
   uv sync --all-packages
   ```

6. Start working!

## Developer Tooling

- Dev Containers
  - Standardised developer environment
- Pre-Commit
  - Run linting and formatting for all files during git commit
- uv
  - Python workspace (`apps/*`) with one shared `uv.lock`
- Ruff and pytest
  - Shared `dev` group; run from the repository root

    ```sh
    uv run ruff check .
    uv run ruff format --check .
    uv run pytest apps
    ```
