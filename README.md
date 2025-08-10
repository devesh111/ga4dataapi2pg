# GA4 Data API to PostgreSQL

This package fetches Google Analytics 4 data via the Data API and stores it in PostgreSQL.

## Install
```bash
pip install -e .
```

## Usage
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
export GA4_PROPERTY_ID=123456789
export PG_DSN=postgresql://user:pass@host:5432/dbname

ga4run --start-date 2024-07-01 --end-date 2024-07-07
```

## Notes
- Ensure GA4 Data API is enabled in Google Cloud.
- Use supported dimensions/metrics combos.
