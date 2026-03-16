import sqlite3
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional

import pandas as pd


@dataclass
class FinanceCleanroomReport:
    db_path: str
    table: str
    column_map: Dict[str, object]
    params: Dict
    row_count: int
    gate_on_count: int
    decision: str
    top_hits: list
    summary_gate0: Dict
    summary_gate1: Dict
    accuracy_status: str
    eval: Dict
    notes: list


def _load_sqlite_table(db_path: str, table: str, cols: List[str]) -> pd.DataFrame:
    con = sqlite3.connect(db_path)
    q = "SELECT " + ", ".join(cols) + f" FROM {table}"
    df = pd.read_sql_query(q, con)
    con.close()
    return df


def _summary(series: pd.Series) -> Dict:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return {"n": 0, "mean": None, "std": None, "p50": None, "p90": None}
    return {
        "n": float(len(s)),
        "mean": float(s.mean()),
        "std": float(s.std(ddof=0)),
        "p50": float(s.quantile(0.5)),
        "p90": float(s.quantile(0.9)),
    }


def _rolling_z(x: pd.Series, m: int) -> pd.Series:
    x = pd.to_numeric(x, errors="coerce")
    mu = x.rolling(m, min_periods=m).mean()
    sd = x.rolling(m, min_periods=m).std(ddof=0)
    return (x - mu) / sd


def _decision_from_gate(gate_on_count: int) -> str:
    return "PASS" if int(gate_on_count) > 0 else "INCOMPLETE"


def _accuracy_status(row_count: int, min_rows: int, has_target: bool) -> str:
    if not has_target:
        return "NO_TARGET"
    if int(row_count) < int(min_rows):
        return "INSUFFICIENT_ROWS"
    return "OK"


def _compute_eval(df: pd.DataFrame, target_col: str) -> Dict:
    """
    Returns confusion + precision/recall/f1 and the timestamps of FP/FN when available.
    Assumes df contains 'gate_on' and optional time column 't'.
    """
    if target_col not in df.columns or "gate_on" not in df.columns:
        return {"enabled": False, "reason": "missing gate_on or target_col"}

    y = pd.to_numeric(df[target_col], errors="coerce").fillna(0).astype(int)
    p = pd.to_numeric(df["gate_on"], errors="coerce").fillna(0).astype(int)

    tp = int(((p == 1) & (y == 1)).sum())
    fp = int(((p == 1) & (y == 0)).sum())
    fn = int(((p == 0) & (y == 1)).sum())
    tn = int(((p == 0) & (y == 0)).sum())

    prec = tp / (tp + fp) if (tp + fp) else None
    rec  = tp / (tp + fn) if (tp + fn) else None
    f1   = (2 * prec * rec / (prec + rec)) if (prec is not None and rec is not None and (prec + rec)) else None

    def _times(mask):
        if "t" in df.columns:
            return df.loc[mask, "t"].astype(str).tolist()
        return []

    false_pos = _times((p == 1) & (y == 0))
    false_neg = _times((p == 0) & (y == 1))

    return {
        "enabled": True,
        "target_col": target_col,
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": prec, "recall": rec, "f1": f1,
        "false_positives": false_pos,
        "false_negatives": false_neg,
    }


def run_cleanroom_finance(db_path: str, table: str, params: Dict) -> Tuple[pd.DataFrame, pd.DataFrame, FinanceCleanroomReport]:
    """
    Supports:
      A) single signal: params: time_col, value_col
      B) dual signal severity: params: time_col, value_cols=[...], weights=[...]
         severity = sum_i w_i * abs(z_i)
         gate_on = severity > z_thr
    Evaluation (optional):
      params.target_col (e.g., "target_gate")
      params.min_rows (default 30) -> accuracy_status marks insufficient rows
    """
    params = params or {}
    time_col = params["time_col"]

    value_col = params.get("value_col")
    value_cols = params.get("value_cols")

    m = int(params.get("m", 60))
    z_thr = float(params.get("z_thr", 1.5))
    gate_name = params.get("gate_name", "value_z")
    top_k = int(params.get("top_k", 5))

    target_col = params.get("target_col", "")
    min_rows = int(params.get("min_rows", 30))

    # -------- severity mode --------
    if value_cols and isinstance(value_cols, list) and len(value_cols) >= 2:
        cols = [time_col] + value_cols + ([target_col] if target_col else [])
        df = _load_sqlite_table(db_path, table, cols).copy()
        df = df.rename(columns={time_col: "t"})

        weights = params.get("weights", [1.0] * len(value_cols))
        if not isinstance(weights, list) or len(weights) != len(value_cols):
            weights = [1.0] * len(value_cols)

        z_cols = []
        for c in value_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            zc = f"z_{c}"
            df[zc] = _rolling_z(df[c], m)
            z_cols.append(zc)

        sev = 0.0
        for w, zc in zip(weights, z_cols):
            sev = sev + float(w) * df[zc].abs()
        df["severity"] = sev

        df["gate_on"] = (df["severity"] > z_thr).fillna(False).astype(int)
        df["instability"] = df["severity"]

        out_S = df[["t", "instability", "gate_on"]].copy()

        gate_on_count = int(df["gate_on"].sum())
        decision = _decision_from_gate(gate_on_count)

        g0 = df.loc[df["gate_on"] == 0, "instability"]
        g1 = df.loc[df["gate_on"] == 1, "instability"]

        # top hits payload
        hits_df = df.loc[df["gate_on"] == 1, ["t"] + value_cols + z_cols + ["severity"]].copy()
        if not hits_df.empty:
            hits_df = hits_df.sort_values("severity", ascending=False).head(top_k)
        top_hits = hits_df.to_dict(orient="records")

        # evaluation
        has_target = bool(target_col) and (target_col in df.columns)
        eval_block = _compute_eval(df, target_col) if has_target else {"enabled": False, "reason": "no target_col"}
        acc_status = _accuracy_status(int(df.shape[0]), min_rows, has_target)

        report = FinanceCleanroomReport(
            db_path=db_path,
            table=table,
            column_map={"time": time_col, "value_cols": value_cols, "target_col": (target_col or None)},
            params={"engine": "finance", "m": m, "z_thr": z_thr, "gate_name": gate_name, "weights": weights, "mode": "severity", "top_k": top_k, "min_rows": min_rows, "target_col": (target_col or "")},
            row_count=int(df.shape[0]),
            gate_on_count=gate_on_count,
            decision=decision,
            top_hits=top_hits,
            summary_gate0=_summary(g0),
            summary_gate1=_summary(g1),
            accuracy_status=acc_status,
            eval=eval_block,
            notes=[
                "Gate is defined as: severity > z_thr where severity = sum_i w_i * abs(zscore(value_i, rolling m)).",
                "All outputs are descriptive; no causal claims are made.",
            ],
        )
        return df, out_S, report

    # -------- single-signal mode --------
    if not value_col:
        raise ValueError("Finance cleanroom requires either value_col or value_cols")

    cols = [time_col, value_col] + ([target_col] if target_col else [])
    df = _load_sqlite_table(db_path, table, cols).copy()
    df = df.rename(columns={time_col: "t", value_col: "value"})
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df["z"] = _rolling_z(df["value"], m)
    df["gate_on"] = (df["z"].abs() > z_thr).fillna(False).astype(int)
    df["instability"] = df["z"].abs()

    out_S = df[["t", "instability", "gate_on"]].copy()

    gate_on_count = int(df["gate_on"].sum())
    decision = _decision_from_gate(gate_on_count)

    g0 = df.loc[df["gate_on"] == 0, "instability"]
    g1 = df.loc[df["gate_on"] == 1, "instability"]

    hits_df = df.loc[df["gate_on"] == 1, ["t", "value", "z", "instability"]].copy()
    if not hits_df.empty:
        hits_df = hits_df.sort_values("instability", ascending=False).head(top_k)
    top_hits = hits_df.to_dict(orient="records")

    has_target = bool(target_col) and (target_col in df.columns)
    eval_block = _compute_eval(df, target_col) if has_target else {"enabled": False, "reason": "no target_col"}
    acc_status = _accuracy_status(int(df.shape[0]), min_rows, has_target)

    report = FinanceCleanroomReport(
        db_path=db_path,
        table=table,
        column_map={"time": time_col, "value": value_col, "target_col": (target_col or None)},
        params={"engine": "finance", "m": m, "z_thr": z_thr, "gate_name": gate_name, "mode": "single", "top_k": top_k, "min_rows": min_rows, "target_col": (target_col or "")},
        row_count=int(df.shape[0]),
        gate_on_count=gate_on_count,
        decision=decision,
        top_hits=top_hits,
        summary_gate0=_summary(g0),
        summary_gate1=_summary(g1),
        accuracy_status=acc_status,
        eval=eval_block,
        notes=[
            "Gate is defined as: abs(zscore(value, rolling m)) > z_thr.",
            "All outputs are descriptive; no causal claims are made.",
        ],
    )
    return df, out_S, report
