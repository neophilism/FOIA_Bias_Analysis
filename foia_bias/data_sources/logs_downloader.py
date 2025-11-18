"""Download FOIA logs from static URLs."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List

import pandas as pd
import requests

from foia_bias.data_sources.base import BaseIngestor, DocumentRecord


class FOIALogsDownloader(BaseIngestor):
    """Grab CSV/XLSX FOIA logs from static URLs and normalize to Parquet."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.output_dir = self.ensure_dir(config.get("output_dir", "data/agency_logs"))

    def download_log(self, url: str, name: str) -> Path:
        """Download the raw file and return its on-disk path."""
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        suffix = Path(url).suffix.lower()
        tmp_path = self.output_dir / f"{name}{suffix}"
        tmp_path.write_bytes(resp.content)
        return tmp_path

    def normalize_log(self, path: Path) -> Path:
        """Convert the source file to Parquet for fast row-wise access."""
        df = pd.read_csv(path) if path.suffix == ".csv" else pd.read_excel(path)
        out_path = path.with_suffix(".parquet")
        df.to_parquet(out_path, index=False)
        return out_path

    def fetch(self) -> Iterator[DocumentRecord]:
        agencies: List[Dict[str, Any]] = self.config.get("agencies", [])
        for agency in agencies:
            if not agency.get("enabled", True):
                continue
            # Download and normalize each agency's log before emitting a
            # lightweight DocumentRecord that points to the Parquet file.
            path = self.download_log(agency["url"], agency["id"])
            parquet_path = self.normalize_log(path)
            yield DocumentRecord(
                source="agency_logs",
                request_id=agency["id"],
                agency=agency.get("name"),
                title=f"FOIA log {agency['name']}",
                description=str(parquet_path),
                date_submitted=None,
                date_done=None,
                requester=None,
                files=[{"path": str(parquet_path)}],
            )
