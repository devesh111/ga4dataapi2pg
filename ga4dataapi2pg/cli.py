import argparse
from .config import load_config
from .auth import get_credentials
from .dataapi_reader import GA4DataAPIReader
from .postgres_writer import PostgresWriter

def main():
    parser = argparse.ArgumentParser(description="Fetch GA4 data and store to PostgreSQL")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--config", help="Path to YAML config file", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    prop = cfg['property_id']
    pg_dsn = cfg['pg_dsn']
    creds = get_credentials()
    reader = GA4DataAPIReader(prop, credentials=creds)
    writer = PostgresWriter(pg_dsn)
    writer.ensure_tables()
    dims = cfg.get('default_dimensions')
    mets = cfg.get('default_metrics')
    rows = reader.run_report(args.start_date, args.end_date, dims, mets)
    n = writer.write_rows(prop, args.start_date, args.end_date, rows)
    print(f"Inserted {n} rows for {prop} {args.start_date} -> {args.end_date}")

if __name__ == "__main__":
    main()
