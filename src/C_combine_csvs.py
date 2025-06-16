#!/usr/bin/env python3
import re
import csv
from pathlib import Path

##############################################################################
# Main filter ----------------------------------------------------------------
##############################################################################

# Create abstracts directory if it doesn't exist
ABSTRACTS_DIR = Path("../abstracts")
ABSTRACTS_DIR.mkdir(exist_ok=True)

ALL_FIELDS = set()

for csv_path in ABSTRACTS_DIR.glob("T*.csv"):
    with csv_path.open(newline="", encoding="utf-8") as fin:
        ALL_FIELDS.update(next(csv.reader(fin)))   # header row only

ALL_FIELDS = list(ALL_FIELDS)                      # keep arbitrary order

##########################################################################
#  write the merged file
##########################################################################
OUT = ABSTRACTS_DIR / "all_records.csv"
with OUT.open("w", newline="", encoding="utf-8") as fout:
    writer = csv.DictWriter(fout, fieldnames=ALL_FIELDS)
    writer.writeheader()

    for csv_path in ABSTRACTS_DIR.glob("T1*.csv"):
        with csv_path.open(newline="", encoding="utf-8") as fin:
            reader = csv.DictReader(fin)
            for row in reader:
                # fill blanks for any missing columns
                full_row = {k: row.get(k, "") for k in ALL_FIELDS}
                writer.writerow(full_row)
