#!/usr/bin/env python3
"""
Download the latest Monthly NPPES V2 dump, filter to active + Delaware (DE),
and save as a CSV in the current folder.
"""

import os, re, io, sys, zipfile, requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

INDEX_URL = "https://download.cms.gov/nppes/NPI_Files.html"
FILTER_STATE = "DE"
OUTPUT_CSV = "nppes_active_DE_v2_latest.csv"
CHUNK_SIZE = 250000  # rows per pandas chunk

# Candidate columns for filtering
DEACTIVATION_COLS = ["NPI Deactivation Date"]
STATE_COLS = ["Provider Business Practice Location Address State",
              "Provider Business Practice Location State",
              "Provider Business Practice Location Address State Name"]

def http_get(url):
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r

def find_latest_monthly_v2():
    resp = http_get(INDEX_URL)
    soup = BeautifulSoup(resp.text, "html.parser")
    links = [a.get("href") for a in soup.find_all("a") if a.get("href")]
    links = [u if u.startswith("http") else urljoin(INDEX_URL, u) for u in links]
    pat = re.compile(r"NPPES_Data_Dissemination.*?(V2|V\.2).*?\.zip$", re.I)
    candidates = [u for u in links if pat.search(u)]
    if not candidates:
        raise RuntimeError("Could not find Monthly V2 file on CMS index page")
    return sorted(candidates, reverse=True)[0]

def main():
    url = find_latest_monthly_v2()
    print("Latest Monthly V2 file:", url)

    # Download zip
    resp = http_get(url)
    zf = zipfile.ZipFile(io.BytesIO(resp.content))

    # Find the main CSV inside
    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    main_csv = next((n for n in csv_names if "npidata_pfile" in n.lower()), None)
    if not main_csv:
        main_csv = max(csv_names, key=lambda n: zf.getinfo(n).file_size)
    print("Using CSV:", main_csv)

    # Truncate output if exists
    open(OUTPUT_CSV, "w").close()
    total = 0
    header_written = False

    with zf.open(main_csv) as f:
        chunks = pd.read_csv(f, dtype=str, chunksize=CHUNK_SIZE, low_memory=False)
        for i, df in enumerate(chunks, start=1):
            df.columns = [c.strip() for c in df.columns]

            # Active only
            deact_col = next((c for c in DEACTIVATION_COLS if c in df.columns), None)
            if deact_col:
                s = df[deact_col].astype(str).str.strip()
                df = df[(s == "") | (s.str.lower().isin(["nan", "none"]))]

            # Delaware only
            state_col = next((c for c in STATE_COLS if c in df.columns), None)
            if state_col:
                df = df[df[state_col].astype(str).str.upper().str.strip() == FILTER_STATE]

            if df.empty:
                continue

            df.to_csv(OUTPUT_CSV, mode="a", index=False, header=(not header_written))
            header_written = True
            total += len(df)
            print(f"[chunk {i}] wrote {len(df)} rows (total {total})")

    print(f"Done. Wrote {total} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)

