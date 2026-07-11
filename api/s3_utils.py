from botocore.exceptions import ClientError
from fastapi import HTTPException, Response


def list_objects(s3, bucket, prefix="", limit=50, continuation_token=None):
    kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": limit}

    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token

    try:
        response = s3.list_objects_v2(**kwargs)
    except ClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    objects = [
        {
            "key": obj["Key"],
            "size_bytes": obj["Size"],
            "last_modified": obj["LastModified"].isoformat()
        }
        for obj in response.get("Contents", [])
    ]

    return {
        "bucket": bucket,
        "prefix": prefix,
        "count": len(objects),
        "objects": objects,
        "is_truncated": response.get("IsTruncated", False),
        "next_token": response.get("NextContinuationToken")
    }


def get_object_response(s3, bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")

        if error_code in ("NoSuchKey", "404"):
            raise HTTPException(status_code=404, detail=f"'{key}' not found in bucket '{bucket}'")

        raise HTTPException(status_code=502, detail=str(exc))

    body = obj["Body"].read()
    content_type = obj.get("ContentType") or "application/octet-stream"

    return Response(content=body, media_type=content_type)
