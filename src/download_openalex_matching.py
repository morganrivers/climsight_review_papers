# -*- coding: utf-8 -*-
"""
Download ALL works for a given OpenAlex topic and save selected metadata to CSV.

Features
--------
* Supports either **primary-topic only** (topic is first) or **any-position**.
* Streams through the cursor‑based API (no page limits).
* Extracts and stores:
  - OpenAlex ID (work URL)
  - Title (`display_name`)
  - DOI
  - Publication year
  - Cited‑by count
  - Journal/source (`host_venue` display name)
  - OA status (`primary_location.is_oa`, `primary_location.oa_status`)
  - Landing page URL & PDF URL (best OA location first, then primary location)
  - Abstract (decoded from `abstract_inverted_index` if present)
* Automatically creates an output file name like `T10004_primary_works.csv`.

Notes
-----
* OpenAlex allows ~200 requests/minute; the script sleeps politely every 20 calls.
* For extremely large topics (hundreds of thousands of works) the CSV may be
  several hundred MB. Use gzip or a database if storage is a concern.
"""
from __future__ import annotations
import pprint
import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Dict, Any, Iterator, Optional

import requests
from tqdm import tqdm

import urllib.parse as up
from pathlib import Path 
import pandas as pd


OPENALEX_BASE = "https://api.openalex.org"
REQUEST_TIMEOUT = 10  # seconds
PER_PAGE = 200        # API max is 200
SLEEP_EVERY = 20      # polite rate limiting
SLEEP_SECONDS = 1


def make_filter(topic_id: int, primary: bool) -> str:
    tid = f"T{topic_id}"
    parts = [
        f"{'primary_topic.id' if primary else 'topic.id'}:{tid}",
        f"publication_year:2010-2025",
        f"has_abstract:true",
        f"has_doi:true",
    ]
    return ",".join(parts)


def work_iter(topic_id: int, primary_only: bool = False) -> Iterator[Dict[str, Any]]:
    """Yield every work JSON for the topic via cursor pagination."""
    filter_str = make_filter(topic_id,primary_only)
    cursor = "*"  # initial cursor
    calls = 0
    while True:

        url = (
            f"{OPENALEX_BASE}/works"
            f"?filter={filter_str},"
            "title.search:(review NOT \"peer review\"),"
            "is_oa:true,"
            "has_fulltext:true"
            f"&per-page={PER_PAGE}"
            f"&cursor={cursor}"
        )
        print("url")
        print(url)

        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        pprint.pprint(resp)

        if resp.status_code != 200:
            raise RuntimeError(f"OpenAlex API error: {resp.status_code} {resp.text[:200]}")
        data = resp.json()
        for item in data.get("results", []):
            yield item
        # pprint.pprint("data")
        # pprint.pprint(data)
        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break
        calls += 1
        if calls % SLEEP_EVERY == 0:
            time.sleep(SLEEP_SECONDS)


def decode_abstract(inv_idx: Optional[dict]) -> Optional[str]:
    if not inv_idx:
        return None
    max_pos = max(pos for positions in inv_idx.values() for pos in positions)
    words = [None] * (max_pos + 1)
    for word, positions in inv_idx.items():
        for pos in positions:
            words[pos] = word
    return " ".join(w for w in words if w is not None) or None

def top_topic_ids(work, k=3):
    """Return up to k topic IDs (strings like 'T10004'), ordered by score."""
    ids = []
    prim = work.get("primary_topic") or {}
    if prim.get("id"):
        ids.append(prim["id"].split("/")[-1])          # keep only numeric tail
    for t in work.get("topics", []):
        tid = t.get("id", "").split("/")[-1]
        if tid and tid not in ids:
            ids.append(tid)
            if len(ids) == k:
                break
    return (ids + [None] * k)[:k]                      # pad to length k

def country_code_string(work):
    codes = set()
    for au in work.get("authorships", []):
        for inst in au.get("institutions", []):
            cc = inst.get("country_code")
            if cc:
                codes.add(cc)
    return ";".join(sorted(codes)) or None

def sdg_string(work):
    sdgs = work.get("sustainable_development_goals") or []
    return ";".join(g["id"].split("/")[-1]                   # e.g. 3, 7, 13
                    for g in sdgs if g.get("score", 0) > 0.4)
def citation_norm_value(work):
    cnp = work.get("citation_normalized_percentile") or {}
    return cnp.get("value")


def sdg_pairs(work, thresh=0.0):
    """
    Return a ;-separated list of id|score pairs for every SDG object.
    e.g. '3|0.95;13|0.42'. Use `thresh=0.4` if you want to keep the 0.4 cut-off.
    """
    sdgs = work.get("sustainable_development_goals") or []
    parts = []
    for g in sdgs:
        sc = g.get("score", 0)
        if sc >= thresh:
            gid = g["id"].split("/")[-1] 
            parts.append(f"{gid}|{sc:.2f}")
    return ";".join(parts) or None


def extract_row(work: Dict[str, Any]) -> Dict[str, Any]:
    best = work.get("best_oa_location") or {}
    primary_loc = work.get("primary_location") or {}

    def url_chain(keys):
        for key in keys:
            val = best.get(key) or primary_loc.get(key)
            if val:
                return val
        return None

    t1, t2, t3 = top_topic_ids(work)
    return {
        "openalex_id": work.get("id"),
        "title": work.get("display_name"),
        "doi": work.get("doi"),
        "publication_year": work.get("publication_year"),
        "cited_by_count": work.get("cited_by_count"),
        "journal": (work.get("host_venue") or {}).get("display_name"),
        "is_oa": best.get("is_oa", primary_loc.get("is_oa")),
        "oa_status": best.get("oa_status", primary_loc.get("oa_status")),
        "landing_url": url_chain(["url", "landing_page_url"]),
        "pdf_url": url_chain(["url_for_pdf", "pdf_url"]),
        "topic_id_1": t1,
        "topic_id_2": t2,
        "topic_id_3": t3,
        "sdg_pairs": sdg_pairs(work),                 # NEW
        "country_codes": country_code_string(work),
        "language": work.get("language"),
        "citation_norm_pct": citation_norm_value(work),   # NEW
        "abstract": decode_abstract(work.get("abstract_inverted_index")),
    }



def download_topic(topic_id: int, primary_only: bool = False):
    # Create abstracts directory if it doesn't exist
    abstracts_dir = Path("../abstracts")
    abstracts_dir.mkdir(exist_ok=True)
    
    out_name = f"T{topic_id}_{'primary' if primary_only else 'any' }_works.csv"
    out_path = abstracts_dir / out_name
    fieldnames = [
        "openalex_id","title","doi","publication_year","cited_by_count",
        "journal","is_oa","oa_status","landing_url","pdf_url",
        "topic_id_1","topic_id_2","topic_id_3",
        "sdg_pairs","country_codes","language","citation_norm_pct",
        "abstract"
    ]


    # 1. Gather already-saved OpenAlex IDs
    already = set()
    mode = "w"
    if out_path.is_file():
        mode = "a"
        try:
            already = set(pd.read_csv(out_path, usecols=["openalex_id"])["openalex_id"].dropna().unique())
            print(f"[info] {len(already):,} rows already in {out_path}")
        except Exception as e:
            print("[warn] Could not read existing file; treating as empty:", e)

    # 2. Write only new rows
    with out_path.open(mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
            writer.writeheader()
        count = 0
        for work in tqdm(work_iter(topic_id, primary_only=primary_only), desc=f"Fetching works for {topic_id}"):
            if work["id"] in already:
                continue
            row = extract_row(work)
            writer.writerow(row)
            count += 1
    return count

def main() -> None:
    parser = argparse.ArgumentParser(description="Download OpenAlex works for a topic.")
    parser.add_argument("topic", type=int, help="Numeric topic ID (e.g. 10004)")
    parser.add_argument("--primary", action="store_true", help="Match only primary_topic.id")
    args = parser.parse_args()

    topic_id = args.topic
    primary_only = args.primary

    out_name = f"T{topic_id}_{'primary' if primary_only else 'any' }_works.csv"
    out_path = Path(out_name)

    # Stream works and write CSV incrementally
    fieldnames = [
        "openalex_id","title","doi","publication_year","cited_by_count",
        "journal","is_oa","oa_status","landing_url","pdf_url",
        "topic_id_1","topic_id_2","topic_id_3",
        "sdg_pairs","country_codes","language","citation_norm_pct",
        "abstract"
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        count = 0
        iterator = work_iter(topic_id, primary_only=primary_only)
        for work in tqdm(iterator, desc="Fetching works"):
            row = extract_row(work)
            writer.writerow(row)
            count += 1

    print(f"Done. Wrote {count:,} works → {out_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("Interrupted by user")
