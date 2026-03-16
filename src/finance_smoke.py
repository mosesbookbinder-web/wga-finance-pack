from __future__ import annotations
from pathlib import Path
import json
import datetime
import pandas as pd

def utc_now_z() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def run_smoke(raw_root: str = "_INBOX_RAW", out_root: str = "outputs/finance_smoke_v1") -> tuple[int, str]:
    """
    Returns (exit_code, report_path)
    exit_code: 0 on PASS/INCOMPLETE, 1 on REFUSE (hard failure)
    """
    inbox = Path(raw_root)
    out_dir = Path(out_root)
    out_dir.mkdir(parents=True, exist_ok=True)

    csvs = sorted(inbox.rglob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)

    report = {
        "ts_utc": utc_now_z(),
        "picked_csv": None,
        "rows": None,
        "cols": None,
        "columns": None,
        "header_mode": None,
        "note": "",
    }

    if not csvs:
        report["note"] = "No CSV files found under _INBOX_RAW."
        report["header_mode"] = "n/a"
        rp = out_dir / "finance_smoke_report.json"
        rp.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return 0, str(rp)

    picked = csvs[0]
    report["picked_csv"] = str(picked)

    try:
        # First attempt: assume header exists
        df = pd.read_csv(picked)
        cols = [str(c) for c in df.columns]

        # Heuristic: if "headers" look like data (percents/digits), re-read headerless
        looks_like_data = any(("%" in c) or c.strip().isdigit() for c in cols)
        if looks_like_data:
            df = pd.read_csv(picked, header=None)
            df.columns = [f"col_{i}" for i in range(df.shape[1])]
            report["header_mode"] = "no_header"
            report["columns"] = list(df.columns)
        else:
            report["header_mode"] = "header"
            report["columns"] = cols

        report["rows"] = int(df.shape[0])
        report["cols"] = int(df.shape[1])
        report["note"] = "Loaded successfully."
        exit_code = 0

    except Exception as e:
        report["note"] = f"Failed to load CSV: {e}"
        report["header_mode"] = "error"
        exit_code = 1

    rp = out_dir / "finance_smoke_report.json"
    rp.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return exit_code, str(rp)
