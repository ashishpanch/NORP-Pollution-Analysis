#!/usr/bin/env python3

from __future__ import annotations
import re
from pathlib import Path
from typing import Optional, Tuple, List
import sys

try:
    import pandas as pd
except ImportError:
    print("pandas is required. Try: pip install pandas", file=sys.stderr)
    raise


# ---------- repo + path discovery ----------

def find_repo_root() -> Path:
    """
    Find the directory that contains both 'migration' and 'clean_scripts'.
    Prefer walking up from this file; fall back to CWD if needed.
    """
    here = Path(__file__).resolve()
    for p in [here] + list(here.parents):
        if (p / "migration").is_dir() and (p / "clean_scripts").is_dir():
            return p
    # Last resort: CWD during execution
    cwd = Path.cwd()
    if (cwd / "migration").is_dir() and (cwd / "clean_scripts").is_dir():
        return cwd
    raise RuntimeError(
        "Could not locate repo root containing both 'migration' and 'clean_scripts'. "
        "Run from the repo or keep this script in clean_scripts/."
    )


ROOT = find_repo_root()
INFLOW_DIR  = ROOT / "migration" / "inflow"
OUTFLOW_DIR = ROOT / "migration" / "outflow"
OUTDIR      = ROOT / "cleaned_migration_data"
OUTDIR.mkdir(parents=True, exist_ok=True)


# ---------- filename → period parsing ----------

YEAR_PATTERNS = [
    re.compile(r"(?P<y1>\d{4})_(?P<y2>\d{4})"),  # e.g., 2020_2021
    re.compile(r"(?P<yy1>\d{2})(?P<yy2>\d{2})"), # e.g., 1516
    re.compile(r"(?P<y>\d{4})"),                 # e.g., 2021 (assume 2020-2021 season)
]

def parse_period_from_name(name: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Returns (period_start, period_end) as full years, or (None, None) if not found.

    Rules:
      - '2020_2021'  -> (2020, 2021)
      - '1516'       -> (2015, 2016)
      - '2021'       -> assume (2020, 2021)
    """
    base = Path(name).stem

    # 1) 2020_2021
    m = YEAR_PATTERNS[0].search(base)
    if m:
        y1, y2 = int(m.group("y1")), int(m.group("y2"))
        return (y1, y2)

    # 2) 1516
    m = YEAR_PATTERNS[1].search(base)
    if m:
        yy1, yy2 = int(m.group("yy1")), int(m.group("yy2"))
        # fix century: assume 2000s
        y1 = 2000 + yy1
        y2 = 2000 + yy2
        return (y1, y2)

    # 3) 2021  -> assume (2020, 2021)
    m = YEAR_PATTERNS[2].search(base)
    if m:
        y = int(m.group("y"))
        return (y - 1, y)

    return (None, None)


# ---------- IO helpers ----------

def list_files(folder: Path, pattern: str) -> List[Path]:
    files = sorted(folder.glob(pattern))
    print(f"[scan] {folder} -> {len(files)} file(s) matched '{pattern}'")
    return files

def safe_read_csv(path: Path) -> pd.DataFrame:
    """
    Read CSV while surviving non-UTF8 encodings and messy rows.
    Tries utf-8, utf-8-sig, cp1252, latin-1.
    """
    enc_try = ("utf-8", "utf-8-sig", "cp1252", "latin-1")
    last_err = None
    for enc in enc_try:
        try:
            df = pd.read_csv(
                path,
                dtype=str,
                encoding=enc,
                engine="python",        # allows non-UTF8
                on_bad_lines="warn"     # or "skip"
            )
            df.columns = [re.sub(r"\s+", " ", c.strip()) for c in df.columns]
            print(f"[read] {path.name} (encoding={enc})")
            return df
        except UnicodeDecodeError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Failed reading {path} with tried encodings {enc_try}: {last_err}")
    """
    Read CSV while surviving non-UTF8 encodings and slightly messy rows.
    Tries several common encodings used in IRS/Excel exports.
    """
    enc_try = ("utf-8", "utf-8-sig", "cp1252", "latin-1")
    last_err = None
    for enc in enc_try:
        try:
            df = pd.read_csv(
                path,
                dtype=str,
                low_memory=False,
                encoding=enc,
                engine="python",       # more forgiving
                on_bad_lines="warn"    # or "skip" if you want to drop offenders
            )
            # normalize column names
            df.columns = [re.sub(r"\s+", " ", c.strip()) for c in df.columns]
            print(f"[read] {path.name} (encoding={enc})")
            return df
        except UnicodeDecodeError as e:
            last_err = e
            continue
        except Exception as e:
            # other parser errors—remember and try next encoding
            last_err = e
            continue
    raise RuntimeError(f"Failed reading {path} with tried encodings {enc_try}: {last_err}")


def write_csv(df: pd.DataFrame, path: Path):
    # Keep index off; don't change dtypes here.
    df.to_csv(path, index=False)
    print(f"[write] {path} ({len(df):,} rows, {len(df.columns)} cols)")


# ---------- cleaning logic ----------

def annotate(df: pd.DataFrame, *, flow: str, src: Path) -> pd.DataFrame:
    start, end = parse_period_from_name(src.name)
    df = df.copy()
    df.insert(0, "flow", flow)
    df.insert(1, "period_start", start)
    df.insert(2, "period_end", end)
    df.insert(3, "source_file", src.name)
    return df

def process_folder(folder: Path, *, flow: str, pattern: str, outdir: Path) -> pd.DataFrame:
    files = list_files(folder, pattern)
    if not files:
        print(f"[warn] No files found in {folder} with pattern {pattern}")
    parts: List[pd.DataFrame] = []
    for f in files:
        print(f"[read] {f.name}")
        df = safe_read_csv(f)
        df = annotate(df, flow=flow, src=f)
        # per-file cleaned output
        ps, pe = df["period_start"].iat[0], df["period_end"].iat[0]
        suffix = f"{ps}_{pe}" if pd.notna(ps) and pd.notna(pe) else "unknown_period"
        out_file = outdir / f"clean_{flow}_{suffix}.csv"
        write_csv(df, out_file)
        parts.append(df)

    if parts:
        combined = pd.concat(parts, ignore_index=True, sort=False)
    else:
        combined = pd.DataFrame()
    return combined


def main():
    print("========== IRS County Migration Cleaning ==========")
    print(f"[paths] ROOT:        {ROOT}")
    print(f"[paths] INFLOW_DIR:  {INFLOW_DIR}")
    print(f"[paths] OUTFLOW_DIR: {OUTFLOW_DIR}")
    print(f"[paths] OUTDIR:      {OUTDIR}")

    if not INFLOW_DIR.exists():
        print(f"[error] Missing folder: {INFLOW_DIR}")
    if not OUTFLOW_DIR.exists():
        print(f"[error] Missing folder: {OUTFLOW_DIR}")

    inflow_all  = process_folder(INFLOW_DIR,  flow="inflow",  pattern="countyinflow*.csv",  outdir=OUTDIR)
    outflow_all = process_folder(OUTFLOW_DIR, flow="outflow", pattern="countyoutflow*.csv", outdir=OUTDIR)

    if not inflow_all.empty:
        write_csv(inflow_all, OUTDIR / "inflow_all.csv")
    else:
        print("[warn] No inflow rows to combine.")

    if not outflow_all.empty:
        write_csv(outflow_all, OUTDIR / "outflow_all.csv")
    else:
        print("[warn] No outflow rows to combine.")

    if not inflow_all.empty or not outflow_all.empty:
        both = pd.concat([df for df in (inflow_all, outflow_all) if not df.empty],
                         ignore_index=True, sort=False)
        write_csv(both, OUTDIR / "migration_all.csv")
    else:
        print("[warn] Nothing processed; migration_all.csv not written.")

    print("=============== DONE =================")

if __name__ == "__main__":
    main()
