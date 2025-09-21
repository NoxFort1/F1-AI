# F1 Strategy AI (OpenF1)

Portfolio project that predicts race strategy using OpenF1 data.

## What this repo contains
- **Data ingestion**: `get_data.py` downloads all available OpenF1
sessions and aggregates per-endpoint CSVs.
- **EDA**: `notebooks/01_eda.ipynb` validates data and defines
targets for modeling.
- **Next steps** (planned): feature engineering, baseline models,
and an offline dashboard.

## Data source
[OpenF1](https://openf1.org/) - public REST API with CSV mode
(`csv=true`). Live endpoints may require a paid account. This
project uses historical data only.

## Quickstart
```bash
python -m venv .venv && . .venv/bin/activate # Windows .venv\Scripts\activate
pip install -U pandas requests pyarrow matplotlib
python get_data.py # aggregates CSVs into data/openf1_full/
# then open notebooks/01_eda.ipynb
```