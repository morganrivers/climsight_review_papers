import pandas as pd
from pathlib import Path

import download_openalex_matching

TOPIC_CSV = "../openalex_ess_topics.csv"
DONE_FILE = "../completed_topics.txt"

def read_done():
    if not Path(DONE_FILE).is_file():
        return set()
    with open(DONE_FILE) as f:
        return set(int(line.strip()) for line in f if line.strip().isdigit())

def append_done(topic_id):
    with open(DONE_FILE, "a") as f:
        f.write(f"{topic_id}\n")

def main():
    topics = pd.read_csv(TOPIC_CSV)
    done = read_done()
    print(f"Already done: {len(done)} topics")
    for _, row in topics.iterrows():
        tid = int(row["topic_id"])
        if tid in done:
            print(f"Skipping {tid} (already done)")
            continue
        print(f"Downloading topic {tid} ...")
        try:
            n = download_openalex_matching.download_topic(tid, primary_only=True)
            print(f"  Downloaded {n} records for topic {tid}")
            append_done(tid)
        except Exception as e:
            print(f"  FAILED for topic {tid}: {e}")

if __name__ == "__main__":
    main()
