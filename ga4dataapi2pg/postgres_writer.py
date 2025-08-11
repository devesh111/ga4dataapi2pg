from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import json
from typing import Iterable, Dict, Any

class PostgresWriter:
    def __init__(self, dsn: str):
        self.engine = create_engine(dsn, pool_pre_ping=True)

    def ensure_tables(self):
        create = '''
        CREATE TABLE IF NOT EXISTS ga4_reports_raw (
          id BIGSERIAL PRIMARY KEY,
          report_start DATE,
          report_end DATE,
          dimensions JSONB,
          metrics JSONB,
          row_hash TEXT UNIQUE,
          inserted_at TIMESTAMP DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS ga4_job_runs (
          id BIGSERIAL PRIMARY KEY,
          property_id TEXT,
          report_start DATE,
          report_end DATE,
          rows_inserted INTEGER,
          status TEXT,
          started_at TIMESTAMP DEFAULT now(),
          finished_at TIMESTAMP
        );
        '''
        with self.engine.begin() as conn:
            conn.execute(text(create))

    def write_rows(self, property_id: str, start_date: str, end_date: str,
                   rows: Iterable[Dict[str, Any]]) -> int:
        inserted = 0
        with self.engine.begin() as conn:
            run_res = conn.execute(text(
                "INSERT INTO ga4_job_runs(property_id, report_start, report_end, rows_inserted, status, started_at) "
                "VALUES(:pid, :s, :e, 0, 'running', now()) RETURNING id"
            ), {"pid": property_id, "s": start_date, "e": end_date})
            run_id = run_res.fetchone()[0]
            for r in rows:
                try:
                    conn.execute(text(
                        "INSERT INTO ga4_reports_raw(report_start, report_end, dimensions, metrics, row_hash) "
                        "VALUES(:s, :e, :dims, :mets, :rh) ON CONFLICT (row_hash) DO NOTHING"
                    ), {
                        "s": start_date,
                        "e": end_date,
                        "dims": json.dumps(r["dimensions"], default=str),
                        "mets": json.dumps(r["metrics"], default=str),
                        "rh": r["row_hash"]
                    })
                    inserted += 1
                except IntegrityError:
                    pass
            conn.execute(text(
                "UPDATE ga4_job_runs SET rows_inserted = :cnt, status = 'finished', finished_at = now() WHERE id = :id"
            ), {"cnt": inserted, "id": run_id})
        return inserted
