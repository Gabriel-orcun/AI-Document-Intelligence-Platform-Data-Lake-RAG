"""Benchmarks /ingest vs /ingest_fast for batch sizes of 1 and 100."""

import argparse
import statistics

import requests

SAMPLE_PARAGRAPH = (
    "The company reported quarterly revenue growth driven by strong demand "
    "in its cloud computing segment, while supply chain constraints continued "
    "to pressure gross margins across the hardware division. Management "
    "reiterated full-year guidance and highlighted ongoing investments in "
    "research and development, particularly in artificial intelligence and "
    "semiconductor manufacturing capacity. Risk factors include currency "
    "fluctuations, regulatory scrutiny in international markets, and "
    "dependence on a limited number of key suppliers for critical components."
)


def make_batch(size):
    """Build a batch of distinct texts for a benchmark run.

    Args: number of texts.
    Returns: list of text strings, each unique so ids don't collide.
    """
    return [f"[doc {i}] {SAMPLE_PARAGRAPH}" for i in range(size)]


def call(base_url, endpoint, batch):
    """Call one ingest endpoint and return its reported elapsed time.

    Args: API base URL, endpoint name, batch of texts.
    Returns: elapsed_seconds reported by the API.
    """
    response = requests.post(f"{base_url}/{endpoint}", json={"data": {"texts": batch}})
    response.raise_for_status()
    return response.json()["elapsed_seconds"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    for batch_size in (1, 100):
        batch = make_batch(batch_size)

        ingest_times = [
            call(args.base_url, "ingest", batch) for _ in range(args.repeats)
        ]
        fast_times = [
            call(args.base_url, "ingest_fast", batch) for _ in range(args.repeats)
        ]

        ingest_med = statistics.median(ingest_times)
        fast_med = statistics.median(fast_times)
        improvement = (1 - fast_med / ingest_med) * 100

        print(f"batch={batch_size}")
        print(f"  /ingest      median: {ingest_med:.4f}s  ({ingest_times})")
        print(f"  /ingest_fast median: {fast_med:.4f}s  ({fast_times})")
        print(f"  improvement: {improvement:.1f}%")


if __name__ == "__main__":
    main()
