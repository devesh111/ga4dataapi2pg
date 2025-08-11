from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
import hashlib
import json
from typing import List, Dict, Any, Iterable
import urllib.parse

def row_hash(dims: Dict[str, Any], mets: Dict[str, Any]) -> str:
    s = json.dumps({'d': dims, 'm': mets}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

class GA4DataAPIReader:
    def __init__(self, property_id: str, credentials=None):
        self.property_id = property_id
        self.client = BetaAnalyticsDataClient(credentials=credentials)

    def run_report(self, start_date: str, end_date: str,
                   dimensions: List[str], metrics: List[str],
                   limit: int = 100000) -> Iterable[Dict[str, Any]]:
        dims = [Dimension(name=d) for d in dimensions]
        mets = [Metric(name=m) for m in metrics]
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=dims,
            metrics=mets,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=limit
        )
        resp = self.client.run_report(request)
        dim_names = [h.name for h in resp.dimension_headers]
        metric_names = [h.name for h in resp.metric_headers]
        for r in resp.rows:
            dim_vals = {n: urllib.parse.unquote(v.value) for n, v in zip(dim_names, r.dimension_values)}
            met_vals = {n: getattr(mv, 'value', mv.value) for n, mv in zip(metric_names, r.metric_values)}
            rh = row_hash(dim_vals, met_vals)
            yield {
                "dimensions": dim_vals,
                "metrics": met_vals,
                "row_hash": rh
            }
