"""Merges new metadata entries into an existing metadata.json on S3."""

import json


def merge_and_write_metadata(s3, bucket, key, new_entries, id_field="file"):
    """Merge new entries into an existing metadata.json instead of overwriting it.

    Args: s3 client, bucket, object key, new entries, field used as unique id.
    Returns: the full merged list of entries.
    """
    existing = []

    try:
        existing = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        pass

    merged = {entry[id_field]: entry for entry in existing}
    merged.update({entry[id_field]: entry for entry in new_entries})

    entries = list(merged.values())

    s3.put_object(
        Bucket=bucket, Key=key, Body=json.dumps(entries, indent=2).encode("utf-8")
    )

    return entries
