import os
import yaml
from pathlib import Path

def load_config(path: str | None = None) -> dict:
    cfg = {}
    if path:
        p = Path(path)
        if p.exists():
            with open(p) as f:
                cfg = yaml.safe_load(f) or {}
    cfg['property_id'] = os.getenv('GA4_PROPERTY_ID', cfg.get('property_id'))
    cfg['pg_dsn'] = os.getenv('PG_DSN', cfg.get('pg_dsn'))
    cfg['credentials'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', cfg.get('credentials'))
    cfg['default_dimensions'] = cfg.get('default_dimensions', ['date'])
    cfg['default_metrics'] = cfg.get('default_metrics', ['activeUsers'])
    return cfg
