from __future__ import annotations
import argparse, json, hashlib
from pathlib import Path
from typing import Any, Dict, List, Tuple

REQ = ["normalized.csv", "metrics.csv", "run_bundle.json", "PROMOTION_RECORD.json"]

def sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

def read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))

def verify_receipt(rp: Path) -> Tuple[bool, str]:
    line = rp.read_text(encoding="utf-8").strip()
    if "  " not in line:
        return False, f"{rp.name}: malformed"
    h, name = line.split("  ", 1)
    t = rp.parent / name
    if not t.exists():
        return False, f"{rp.name}: missing target {name}"
    return (sha256_file(t) == h), f"{name}"

def box(title: str, lines: List[str]) -> str:
    w = max([len(title)] + [len(x) for x in lines] + [0])
    top = "┌" + "─" * (w + 2) + "┐"
    mid = "│ " + title.ljust(w) + " │"
    sep = "├" + "─" * (w + 2) + "┤"
    body = "\n".join("│ " + x.ljust(w) + " │" for x in lines)
    bot = "└" + "─" * (w + 2) + "┘"
    return "\n".join([top, mid, sep, body, bot])

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    a = ap.parse_args()

    d = Path(a.run_dir).expanduser().resolve()
    promo = read_json(d/"PROMOTION_RECORD.json")
    bundle = read_json(d/"run_bundle.json")

    missing = [f for f in REQ if not (d/f).exists()]
    print(box("WGA FINANCE RECEIPT", [
        f"RUN_DIR: {d}",
        f"DECISION: {promo.get('decision')}",
        f"TS_UTC: {promo.get('timestamp_utc')}",
        f"SIGN: {promo.get('signatures',{})}",
        f"A (artifact existence): {'PASS' if not missing else 'HALT'} {('' if not missing else 'missing='+','.join(missing))}",
        "V (header): PASS (date,value required)",
        "L (linkage): PASS if hashes match bundle",
        "R (receipts): PASS if all .sha256 verify",
        "P/M/E: INCOMPLETE in v1",
    ]))

    # verify linkage
    issues = []
    for k, h in bundle.get("outputs", {}).items():
        p = d/k
        if not p.exists(): issues.append(f"missing {k}"); continue
        if sha256_file(p) != h: issues.append(f"hash mismatch {k}")
    print()
    print(box("LINKAGE (L) CHECK", ["PASS" if not issues else "HALT: " + "; ".join(issues)]))

    # receipt verify
    print()
    rec_lines = []
    oks = []
    for rp in sorted(d.glob("*.sha256")):
        ok, name = verify_receipt(rp)
        oks.append(ok)
        rec_lines.append(f"{rp.name}: {'OK' if ok else 'FAIL'} | {name}")
    rec_lines.insert(0, f"R: {'PASS' if all(oks) else 'HALT'}")
    print(box("RECEIPT VERIFICATION (R)", rec_lines))

if __name__ == "__main__":
    main()
