import typer
from .config import load_config
from .auth import get_credentials
from .dataapi_reader import GA4DataAPIReader
from .postgres_writer import PostgresWriter

app = typer.Typer()

@app.command()
def run(start_date: str, end_date: str, config: str = None):
    cfg = load_config(config)
    prop = cfg['property_id']
    pg_dsn = cfg['pg_dsn']
    creds = get_credentials()
    reader = GA4DataAPIReader(prop, credentials=creds)
    writer = PostgresWriter(pg_dsn)
    writer.ensure_tables()
    dims = cfg.get('default_dimensions')
    mets = cfg.get('default_metrics')
    rows = reader.run_report(start_date, end_date, dims, mets)
    n = writer.write_rows(prop, start_date, end_date, rows)
    typer.echo(f"Inserted {n} rows for {prop} {start_date} -> {end_date}")

if __name__ == "__main__":
    app()
