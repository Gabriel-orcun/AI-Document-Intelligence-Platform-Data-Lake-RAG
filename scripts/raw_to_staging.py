"""Validates raw documents and copies them into the staging bucket."""

import sys
import boto3
import hashlib
import argparse
from io import BytesIO
from pathlib import Path
from PIL import Image
from tqdm import tqdm

# Makes "scripts" importable whether this file is run directly
# (python scripts/raw_to_staging.py) or imported as a package submodule.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.s3_metadata import merge_and_write_metadata


def sha256_file(content):
    """Hash file bytes.

    Args: raw bytes.
    Returns: hex sha256 digest.
    """
    return hashlib.sha256(content).hexdigest()


def list_s3_files(s3, bucket, prefix):
    """List every object key under a prefix.

    Args: s3 client, bucket name, key prefix.
    Returns: list of object keys, folders excluded.
    """
    files = []

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("/"):
                files.append(obj["Key"])

    return files


def copy_to_staging(s3, raw_bucket, staging_bucket, source_key, destination_key):
    """Copy one object from raw to staging as-is.

    Args: s3 client, raw bucket, staging bucket, source key, destination key.
    Returns: the object bytes.
    """
    obj = s3.get_object(Bucket=raw_bucket, Key=source_key)

    content = obj["Body"].read()

    s3.put_object(Bucket=staging_bucket, Key=destination_key, Body=content)

    return content


def process_financial(s3, raw_bucket, staging_bucket):
    """Validate financial document images and stage the valid ones.

    Args: s3 client, raw bucket, staging bucket.
    Returns: none, writes staged files and financial/metadata.json.
    """
    metadata = []

    prefixes = [
        "financial/train/images/",
        "financial/validation/images/",
        "financial/test/images/",
    ]

    for prefix in prefixes:

        files = list_s3_files(s3, raw_bucket, prefix)

        for file in tqdm(files, desc=prefix):

            content = s3.get_object(Bucket=raw_bucket, Key=file)["Body"].read()

            try:
                image = Image.open(BytesIO(content))

                width, height = image.size

                valid = True

                image_format = image.format

            except Exception:
                width = None
                height = None
                valid = False
                image_format = None

            filename = Path(file).name

            label = filename.split("_")[0]

            destination = file.replace("financial/", "financial/")

            if valid:

                s3.put_object(Bucket=staging_bucket, Key=destination, Body=content)

            metadata.append(
                {
                    "document_id": sha256_file(content),
                    "file": filename,
                    "path": destination,
                    "label": label,
                    "format": image_format,
                    "width": width,
                    "height": height,
                    "size_bytes": len(content),
                    "sha256": sha256_file(content),
                    "status": "ready_for_ocr" if valid else "invalid",
                }
            )

    merge_and_write_metadata(
        s3, staging_bucket, "financial/metadata.json", metadata, id_field="path"
    )


def process_sec(s3, raw_bucket, staging_bucket, keys=None):
    """Validate SEC filings and stage the valid ones.

    Args: s3 client, raw bucket, staging bucket, optional explicit key list.
    Returns: the metadata entries written for the processed files.
    """
    metadata = []

    files = keys if keys is not None else list_s3_files(s3, raw_bucket, "sec_edgar/")

    for file in tqdm(files, desc="SEC EDGAR"):

        content = s3.get_object(Bucket=raw_bucket, Key=file)["Body"].read()

        if file.endswith(".html"):

            valid = len(content) > 0 and b"<html" in content.lower()

            destination = file

            if valid:

                s3.put_object(Bucket=staging_bucket, Key=destination, Body=content)

            metadata.append(
                {
                    "document_id": sha256_file(content),
                    "file": file,
                    "size_bytes": len(content),
                    "sha256": sha256_file(content),
                    "status": ("ready_for_extraction" if valid else "invalid"),
                }
            )

    merge_and_write_metadata(
        s3, staging_bucket, "sec_edgar/metadata.json", metadata, id_field="file"
    )

    return metadata


def main():
    """CLI entry point: run process_financial then process_sec.

    Args: none, reads CLI flags.
    Returns: none.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--endpoint-url", default="http://localhost:4566")

    parser.add_argument("--raw-bucket", default="raw")

    parser.add_argument("--staging-bucket", default="staging")

    args = parser.parse_args()

    s3 = boto3.client("s3", endpoint_url=args.endpoint_url)

    process_financial(s3, args.raw_bucket, args.staging_bucket)

    process_sec(s3, args.raw_bucket, args.staging_bucket)

    print("RAW -> STAGING terminé")


if __name__ == "__main__":
    main()
