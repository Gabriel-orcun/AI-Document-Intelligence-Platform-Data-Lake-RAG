"""Downloads SEC EDGAR 10-K filings and uploads them to the raw bucket."""

import json
import time
import argparse
import requests
import boto3
from pathlib import Path
from tqdm import tqdm

SEC_HEADERS = {"User-Agent": "FinancialDocumentLake contact@email.com"}


def get_company_list():
    """Fetch the full SEC ticker/CIK directory.

    Args: none.
    Returns: list of dicts with cik, ticker, name.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    response = requests.get(url, headers=SEC_HEADERS)
    response.raise_for_status()

    data = response.json()

    companies = []

    for item in data.values():
        companies.append(
            {
                "cik": str(item["cik_str"]).zfill(10),
                "ticker": item["ticker"],
                "name": item["title"],
            }
        )

    return companies


def get_company_filings(cik):
    """List a company's 10-K filings.

    Args: company CIK.
    Returns: list of dicts with accession, document, date.
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"

    response = requests.get(url, headers=SEC_HEADERS)

    if response.status_code != 200:
        return []

    data = response.json()
    filings = data["filings"]["recent"]

    documents = []

    for form, accession, document, date in zip(
        filings["form"],
        filings["accessionNumber"],
        filings["primaryDocument"],
        filings["filingDate"],
    ):
        if form == "10-K":
            documents.append(
                {"accession": accession, "document": document, "date": date}
            )

    return documents


def download_document(cik, ticker, filing, output_dir):
    """Download one filing HTML to disk.

    Args: company CIK, ticker, filing dict, local output directory.
    Returns: tuple (success bool, local file path or None).
    """
    accession_clean = filing["accession"].replace("-", "")

    url = (
        "https://www.sec.gov/Archives/"
        f"edgar/data/{int(cik)}/"
        f"{accession_clean}/"
        f"{filing['document']}"
    )

    company_dir = Path(output_dir) / "documents" / ticker
    company_dir.mkdir(parents=True, exist_ok=True)

    filename = company_dir / f"{filing['date']}_10K.html"

    response = requests.get(url, headers=SEC_HEADERS)

    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
        return True, filename

    return False, None


def ingest_sec_documents(output_dir, max_documents):
    """Download 10-K filings for companies until the limit is reached.

    Args: local output directory, max number of documents to download.
    Returns: none, writes files and a local metadata.json.
    """
    companies = get_company_list()

    print(f"{len(companies)} entreprises trouvées")

    downloaded = 0
    metadata = []

    for company in tqdm(companies):
        if downloaded >= max_documents:
            break

        filings = get_company_filings(company["cik"])

        for filing in filings:
            if downloaded >= max_documents:
                break

            success, filepath = download_document(
                company["cik"], company["ticker"], filing, output_dir
            )

            if success:
                metadata.append(
                    {
                        "company": company["name"],
                        "ticker": company["ticker"],
                        "date": filing["date"],
                        "type": "10-K",
                        "file": str(filepath.relative_to(output_dir)),
                    }
                )

                downloaded += 1
                print(f"{downloaded}/{max_documents}")

        time.sleep(0.15)

    metadata_file = Path(output_dir) / "metadata.json"

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"{downloaded} documents téléchargés")


def upload_to_s3(local_dir, bucket, prefix, endpoint_url):
    """Upload every local file to S3 under a prefix.

    Args: local directory, bucket name, key prefix, S3 endpoint URL.
    Returns: none.
    """
    s3 = boto3.client("s3", endpoint_url=endpoint_url)

    local_dir = Path(local_dir)

    for file in local_dir.rglob("*"):
        if file.is_file():

            relative_path = file.relative_to(local_dir)

            s3_key = f"{prefix}/{relative_path}"

            print(f"Upload s3://{bucket}/{s3_key}")

            s3.upload_file(str(file), bucket, s3_key)

    print("Upload S3 terminé")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--output-dir", default="data/raw/sec")

    parser.add_argument("--limit", type=int, default=30)

    parser.add_argument("--bucket", default="raw")

    parser.add_argument("--prefix", default="sec_edgar")

    parser.add_argument("--endpoint-url", default="http://localhost:4566")

    args = parser.parse_args()

    ingest_sec_documents(args.output_dir, args.limit)

    upload_to_s3(args.output_dir, args.bucket, args.prefix, args.endpoint_url)
