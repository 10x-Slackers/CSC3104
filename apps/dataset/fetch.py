"""Load the IBM AML (Altman) dataset from Kaggle as a pandas DataFrame.

Public dataset, no Kaggle login needed. kagglehub caches files in
~/.cache/kagglehub

    uv sync --all-packages
    uv run apps/dataset/fetch.py                     # HI-Small_Trans.csv (~476 MB first time)
    uv run apps/dataset/fetch.py LI-Small_Trans.csv
"""

import sys

import kagglehub
from kagglehub import KaggleDatasetAdapter

DATASET = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml/versions/8"

# Text, not int: Trans/Patterns zero-pad bank IDs ("010"); the second "Account" header becomes "Account.1".
ID_COLS = {"From Bank": str, "To Bank": str, "Account": str, "Account.1": str}


def load(file="HI-Small_Trans.csv"):
    return kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS, DATASET, file, pandas_kwargs={"dtype": ID_COLS}
    )


if __name__ == "__main__":
    df = load(*sys.argv[1:])
    print(df.shape)
    print(df.head())
