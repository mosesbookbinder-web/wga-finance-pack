from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple


def utc_now_z() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(p: Path) -> str:
    return sha256_bytes(p.read_bytes())


def write_text(path: Path, s: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(s, encoding="utf-8")


def write_json(path: Path, obj: Any) -> None:
    payload = json.dumps(obj, indent=4, sort_keys=True).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def write_sha256_receipt(path: Path) -> str:
    h = sha256_file(path)
    write_text(path.parent / (path.name + ".sha256"), f"{h}  {path.name}\n")
    return h


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        if r.fieldnames is None:
            raise ValueError("CSV has no header row")
        rows = [row for row in r]
    return rows


def require_cols(rows: List[Dict[str, str]], required: List[str]) -> None:
    if not rows:
        raise ValueError("CSV has no data rows")
    cols = set(rows[0].keys())
    missing = [c for c in required if c not in cols]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def parse_float(x: str) -> float:
    return float(x.strip())


def rolling_mean(xs: List[float], w: int) -> List[float | None]:
    out: List[float | None] = [None] * len(xs)
    if w <= 0:
        return out
    s = 0.0
    for i, v in enumerate(xs):
        s += v
        if i >= w:
            s -= xs[i - w]
        if i >= w - 1:
            out[i] = s / w
    return out


def rolling_std(xs: List[float], w: int) -> List[float | None]:
    out: List[float | None] = [None] * len(xs)
    if w <= 1:
        return out
    for i in range(len(xs)):
        if i < w - 1:
            continue
        window = xs[i - w + 1 : i + 1]
        m = sum(window) / w
        var = sum((z - m) ** 2 for z in window) / (w - 1)
        out[i] = var ** 0.5
    return out


@dataclass(frozen=True)
class Config:
    window: int = 20
    eps: float = 1e-12
    version: str = "WGA-FINANCE-PACK-0.1.0"
    signatures_operator: str = "bookbinder5"
    signatures_cosign: str = "K5"


def compute_metrics(dates: List[str], values: List[float], cfg: Config) -> List[Dict[str, Any]]:
    # simple return: r_t = (x_t / x_{t-1}) - 1
    rets: List[float | None] = [None]
    for i in range(1, len(values)):
        prev = values[i - 1]
        rets.append((values[i] / prev) - 1.0 if abs(prev) > cfg.eps else None)

    # rolling vol on returns (std)
    r_clean = [0.0 if r is None else float(r) for r in rets]  # keep length stable
    vol = rolling_std(r_clean, cfg.window)

    # rolling z-score on values
    mu = rolling_mean(values, cfg.window)
    sd = rolling_std(values, cfg.window)
    z: List[float | None] = [None] * len(values)
    for i in range(len(values)):
        if mu[i] is None or sd[i] is None or sd[i] < cfg.eps:
            continue
        z[i] = (values[i] - mu[i]) / sd[i]

    # v1 Bookbinder-ish instability proxy:
    # S_t = |z_t| + vol_t  (both optional)
    S: List[float | None] = [None] * len(values)
    for i in range(len(values)):
        if z[i] is None and vol[i] is None:
            continue
        a = 0.0 if z[i] is None else abs(float(z[i]))
        b = 0.0 if vol[i] is None else float(vol[i])
        S[i] = a + b

    out: List[Dict[str, Any]] = []
    for i in range(len(values)):
        out.append({
            "date": dates[i],
            "value": values[i],
            "ret": rets[i],
            "roll_vol": vol[i],
            "roll_z": z[i],
            "S_t": S[i],
        })
    return out


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


def main() -> None:
    ap = argparse.ArgumentParser(prog="wga-finance", description="WGA Finance Pack v1: ingest + metrics + receipts")
    ap.add_argument("--in-csv", required=True, help="Input CSV with columns: date,value")
    ap.add_argument("--outdir", required=True, help="Output run directory")
    ap.add_argument("--window", type=int, default=20, help="Rolling window")
    args = ap.parse_args()

    cfg = Config(window=args.window)
    in_csv = Path(args.in_csv).expanduser().resolve()
    outdir = Path(args.outdir).expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    # Gate V: header complete
    rows = read_csv_rows(in_csv)
    require_cols(rows, ["date", "value"])

    # Normalize
    dates: List[str] = []
    values: List[float] = []
    for r in rows:
        dates.append(r["date"])
        values.append(parse_float(r["value"]))

    normalized_rows = [{"date": d, "value": v} for d, v in zip(dates, values)]
    norm_path = outdir / "normalized.csv"
    write_csv(norm_path, normalized_rows, ["date", "value"])
    h_norm = write_sha256_receipt(norm_path)

    # Metrics
    metrics = compute_metrics(dates, values, cfg)
    metrics_path = outdir / "metrics.csv"
    write_csv(metrics_path, metrics, ["date", "value", "ret", "roll_vol", "roll_z", "S_t"])
    h_metrics = write_sha256_receipt(metrics_path)

    # Bundle (Linkage)
    bundle = {
        "run_meta": {
            "timestamp_utc": utc_now_z(),
            "cwd": str(Path.cwd()),
        },
        "config": {
            "version": cfg.version,
            "window": cfg.window,
        },
        "inputs": {
            "in_csv": str(in_csv),
            "in_csv_sha256": sha256_file(in_csv),
            "rows": len(rows),
        },
        "outputs": {
            "normalized.csv": h_norm,
            "metrics.csv": h_metrics,
        },
        "decision": "PASS",
    }
    bundle_path = outdir / "run_bundle.json"
    write_json(bundle_path, bundle)
    h_bundle = write_sha256_receipt(bundle_path)

    promo = {
        "promotion_version": "WGA-FINANCE-PROMO-1.0",
        "timestamp_utc": utc_now_z(),
        "decision": "PASS",
        "first_refusal": None,
        "counts": {"rows": len(rows)},
        "outputs": {
            "normalized.csv": h_norm,
            "metrics.csv": h_metrics,
            "run_bundle.json": h_bundle,
        },
        "signatures": {"operator": cfg.signatures_operator, "cosign": cfg.signatures_cosign},
    }
    promo_path = outdir / "PROMOTION_RECORD.json"
    write_json(promo_path, promo)
    write_sha256_receipt(promo_path)

    print("WROTE", norm_path)
    print("WROTE", metrics_path)
    print("WROTE", bundle_path)
    print("WROTE", promo_path)

