"""Splits the financial document dataset into train/val/test and uploads it to S3."""

import argparse
import json
import random
import shutil
from pathlib import Path

import boto3

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg"}


def extract_label(filename):
    """Get the class label from a filename.

    Args: filename.
    Returns: label string (text before the first underscore).
    """

    return filename.split("_")[0]


def split_dataset(source_dir, output_dir, train_ratio=0.8, val_ratio=0.1, seed=42):
    """Split images by label into train/validation/test folders.

    Args: source directory, output directory, train ratio, val ratio, seed.
    Returns: none, writes images and annotations.json per split.
    """
    random.seed(seed)

    source_dir = Path(source_dir)
    output_dir = Path(output_dir)

    images = [f for f in source_dir.iterdir() if f.suffix.lower() in IMAGE_EXTENSIONS]

    print(f"{len(images)} images trouvées")

    # Groupement par label
    classes = {}

    for image in images:

        label = extract_label(image.name)

        classes.setdefault(label, []).append(image)

    print(f"{len(classes)} classes trouvées")

    splits = {"train": [], "validation": [], "test": []}

    # Split par classe pour garder l'équilibre
    for label, files in classes.items():

        random.shuffle(files)

        total = len(files)

        train_end = int(total * train_ratio)

        val_end = int(total * (train_ratio + val_ratio))

        splits["train"].extend([(f, label) for f in files[:train_end]])

        splits["validation"].extend([(f, label) for f in files[train_end:val_end]])

        splits["test"].extend([(f, label) for f in files[val_end:]])

    # Création fichiers

    for split, data in splits.items():

        images_dir = output_dir / split / "images"

        images_dir.mkdir(parents=True, exist_ok=True)

        annotations = []

        for image, label in data:

            destination = images_dir / image.name

            shutil.copy2(image, destination)

            annotations.append({"file": image.name, "label": label})

        with open(output_dir / split / "annotations.json", "w", encoding="utf-8") as f:

            json.dump(annotations, f, indent=4)

        print(split, ":", len(annotations), "images")


def upload_s3(local_dir, bucket, prefix, endpoint):
    """Upload every local file to S3 under a prefix.

    Args: local directory, bucket name, key prefix, S3 endpoint URL.
    Returns: none.
    """
    s3 = boto3.client("s3", endpoint_url=endpoint)

    local_dir = Path(local_dir)

    for file in local_dir.rglob("*"):

        if file.is_file():

            relative = file.relative_to(local_dir)

            key = prefix + "/" + str(relative)

            print(f"Upload {key}")

            s3.upload_file(str(file), bucket, key)

    print("Upload S3 terminé")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Split dataset financier et upload S3")

    parser.add_argument(
        "--source-dir",
        type=str,
        required=True,
        help="Dossier contenant les images financières",
    )

    parser.add_argument(
        "--output-dir", type=str, required=True, help="Dossier de sortie train/val/test"
    )

    parser.add_argument("--bucket", type=str, default="raw")

    parser.add_argument("--prefix", type=str, default="financial")

    parser.add_argument("--endpoint", type=str, default="http://localhost:4566")

    args = parser.parse_args()

    split_dataset(source_dir=args.source_dir, output_dir=args.output_dir)

    upload_s3(
        local_dir=args.output_dir,
        bucket=args.bucket,
        prefix=args.prefix,
        endpoint=args.endpoint,
    )
