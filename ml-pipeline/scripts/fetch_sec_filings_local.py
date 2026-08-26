"""
Fetches SEC 10-K filings locally and uploads them to the Databricks Volume.

Why local, not a Databricks notebook: this workspace's serverless compute has
no general internet DNS resolution (confirmed — data.sec.gov failed to
resolve from a notebook), so the actual HTTP fetch from SEC EDGAR has to
happen from a machine with normal internet access, with the result uploaded
via `databricks fs cp` afterward. Databricks/AWS-hosted calls (embeddings,
Model Serving, boto3) are unaffected — those go through allowed endpoints.

Run: infra-deploy/venv/Scripts/python.exe ml-pipeline/scripts/fetch_sec_filings_local.py
Requires the Databricks CLI authenticated (DATABRICKS_HOST / DATABRICKS_TOKEN
env vars) to upload the results afterward.
"""
import json
import subprocess
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

USER_AGENT = "Victor Kamya vic1771@hotmail.com"  # matches SEC_EDGAR_USER_AGENT in deploy.py
VOLUME_PATH = "dbfs:/Volumes/ml/ml_artifacts/rag_data/sec_filings"
OUT_DIR = Path(__file__).parent / "_sec_filings_tmp"

TICKER_TO_CIK = {
    "AAPL": "0000320193",
    "MSFT": "0000789019",
    "NVDA": "0001045810",
}


def fetch_latest_10k_url(cik: str) -> tuple[str, str]:
    resp = requests.get(
        f"https://data.sec.gov/submissions/CIK{cik}.json",
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    resp.raise_for_status()
    filings = resp.json()["filings"]["recent"]

    for i, form in enumerate(filings["form"]):
        if form == "10-K":
            accession = filings["accessionNumber"][i].replace("-", "")
            primary_doc = filings["primaryDocument"][i]
            filing_date = filings["filingDate"][i]
            url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{primary_doc}"
            return filing_date, url

    raise ValueError(f"No 10-K found for CIK {cik}")


def fetch_filing_text(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text(separator="\n")


def main():
    OUT_DIR.mkdir(exist_ok=True)
    results = {}

    for ticker, cik in TICKER_TO_CIK.items():
        print(f"Fetching latest 10-K for {ticker} (CIK {cik})...")
        filing_date, url = fetch_latest_10k_url(cik)
        text = fetch_filing_text(url)

        local_path = OUT_DIR / f"{ticker}_10K_{filing_date}.txt"
        local_path.write_text(text, encoding="utf-8")
        results[ticker] = {"filing_date": filing_date, "url": url, "chars": len(text)}
        print(f"  saved {len(text):,} chars -> {local_path}")
        time.sleep(1)  # SEC fair-access pacing

    print(json.dumps(results, indent=2))

    print(f"\nUploading to {VOLUME_PATH} ...")
    for local_path in OUT_DIR.glob("*.txt"):
        dest = f"{VOLUME_PATH}/{local_path.name}"
        subprocess.run(
            ["databricks", "fs", "cp", str(local_path), dest, "--overwrite"],
            check=True,
        )
        print(f"  uploaded {local_path.name}")


if __name__ == "__main__":
    main()
