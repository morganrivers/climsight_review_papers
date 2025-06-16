#!/usr/bin/env python3
"""
fetch_fulltext.py
─────────────────
• Read a Scopus-like CSV that has columns: Title, doi (or DOI), decision …
• Only rows whose decision == "INCLUDE" are processed.
• Tries several public APIs (Unpaywall → Semantic Scholar → OpenAlex → CORE)
  and finally a DOI HEAD redirect to locate an OA PDF.
• Handles MDPI and Elsevier quirks automatically.
• If no PDF is captured *but* `oa_status == yes` and you have a
  publication_url, it saves the HTML page instead (to htmls/).
• Creates   pdfs/   and   htmls/   directories beside the script.
• Lots of print lines so you can see *everything* that happens.
"""

import csv, os, re, time, json, urllib.parse
from pathlib import Path
from urllib.parse import urlparse

import requests
from urllib3.exceptions import NameResolutionError

from io import BytesIO
import fitz  # PyMuPDF
import shutil

import pprint
# ───────────────────────── configurable paths ────────────────────────────────
CSV_IN   = "../abstracts/all_records.csv"

# Create fulltexts directory and subdirectories
FULLTEXTS_DIR = Path("../fulltexts")
ELSEVIER_PDF_DIR = FULLTEXTS_DIR / "elsevier_pdfs"
PDF_DIR = FULLTEXTS_DIR / "pdfs"

# Create directories if they don't exist
FULLTEXTS_DIR.mkdir(exist_ok=True)
ELSEVIER_PDF_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(exist_ok=True)
FIELDNAMES = ["tag", "doi", "oa_status",
              "elsevier_error_code", "elsevier_pages","elsevier_status", "success",
              "unpaywall_status", "semantic_status",
              "openalex_status", "core_status",
              "doi_head_status","direct_download_status"]
STATS_CSV = "../fulltexts/scraping_stats.csv"


# Load API key from API_KEYS.txt file
def load_api_key():
    try:
        with open('../API_KEYS.txt', 'r') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    if line.startswith('ELSEVIER_API_KEY='):
                        return line.split('=', 1)[1].strip()
        raise ValueError("ELSEVIER_API_KEY not found in API_KEYS.txt")
    except FileNotFoundError:
        raise FileNotFoundError("API_KEYS.txt file not found. Please create it with your Elsevier API key.")


def get_elsevier_headers():
    api_key = load_api_key()
    return {
        "Accept": "application/pdf",
        "X-ELS-APIKey": api_key,
    }



# ────────────────────────── common helpers ───────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36"
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Load email from API_KEYS.txt file
def load_email():
    try:
        with open('../API_KEYS.txt', 'r') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    if line.startswith('EMAIL_ADDRESS='):
                        return line.split('=', 1)[1].strip()
        raise ValueError("EMAIL_ADDRESS not found in API_KEYS.txt")
    except FileNotFoundError:
        raise FileNotFoundError("API_KEYS.txt file not found. Please create it with your email address.")

def write_stats(row):
    with open(STATS_CSV, "a", newline="", encoding="utf-8") as fh:
        csv.DictWriter(fh, fieldnames=FIELDNAMES).writerow(row)



def is_full_article(pdf_file) -> bool:
    """
    True  → looks like a complete article
    False → probably just the title/abstract page

    • Primary test: ≥ `min_pages` pages using PyPDF2
    • Fallback:     at least `min_len` bytes if PyPDF2 unavailable
    """

    doc = fitz.open(pdf_file)
    num_pages = len(doc)
    print(f"num_pages: {num_pages}")
    return num_pages >= 2


def looks_like_pdf(blob: bytes) -> bool:
    return blob[:4] == b"%PDF" and len(blob) > 10_000


def dbg(msg: str):
    print(msg, flush=True)

def safe_filename(text: str, limit: int = 80) -> str:
    return re.sub(r"[^\w\-]+", "_", text)[:limit]

def sanitize_doi(raw: str | None) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    raw = re.sub(r"https?://(dx\.)?doi\.org/", "", raw)
    return re.sub(r"\s+", "", raw)

def fetch_json(url: str) -> dict | None:
    dbg(f"  GET-JSON  {url}")
    try:
        r = SESSION.get(url, timeout=20)
        dbg(f"    ↪ {r.status_code}  {r.reason}  len={len(r.content)}")
        r.raise_for_status()
        return r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        dbg(f"    ! JSON error: {e}")
        return None

# ──────────────────────────── locators ───────────────────────────────────────
def url_unpaywall(doi):
    j = fetch_json(f"https://api.unpaywall.org/v2/{doi}?email={load_email()}")
    if not j:
        return None
    loc = j.get("best_oa_location")
    if isinstance(loc, dict):
        return loc.get("url_for_pdf")
    return None

def url_semantic(doi):
    j = fetch_json(
        f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}"
        "?fields=openAccessPdf"
    )
    return (j or {}).get("openAccessPdf", {}).get("url")

def url_openalex(doi):
    j = fetch_json(
        f"https://api.openalex.org/works/doi:{doi}?email={load_email()}"
    )
    return (j or {}).get("oa_status", {}).get("oa_url")

def url_core_doi(doi):
    q = urllib.parse.quote_plus(f"doi:{doi}")
    j = fetch_json(f"https://api.core.ac.uk/v3/search/works?q={q}")
    hits = (j or {}).get("results", [])
    return hits[0]["downloadUrl"] if hits else None

def url_doi_head(doi):
    url = f"https://doi.org/{doi}"
    dbg(f"  HEAD/doi  {url}")
    try:
        r = SESSION.head(url, allow_redirects=False, timeout=20)
        dbg(f"    ↪ {r.status_code}  Location={r.headers.get('Location')}")
        if r.status_code in (302, 303):
            return r.headers.get("Location")
    except requests.RequestException as e:
        dbg(f"    ! doi HEAD error: {e}")
    return None

LOCATORS = [
    ("unpaywall_status", url_unpaywall),
    ("semantic_status", url_semantic),
    ("openalex_status", url_openalex),
    ("core_status", url_core_doi),
    ("doi_head_status", url_doi_head)
]

# ─────────────────── provider-specific URL tweaks ────────────────────────────
pii_re = re.compile(r"/retrieve/pii/(S\d{15,})")
def fix_elsevier(url):
    m = pii_re.search(url)
    if not m:
        return url
    pii = m.group(1)
    return f"https://reader.elsevier.com/reader/sd/pii/{pii}?download=true"

def maybe_fix_mdpi(url, html_url):
    if "www.mdpi.com" in url and url.endswith("/pdf"):
        return url, {"Referer": html_url or "https://www.mdpi.com/"}
    return url, {}

BAD_TLS = {"mausamjournal.imd.gov.in"}   # hostnames whose certs are broken

# ───────────────────────── download helpers ─────────────────────────────────
def download(url: str, dest: Path, html_ref: str | None = None) -> bool:
    # MDPI referer tweak
    url, extra_hdr = maybe_fix_mdpi(url, html_ref)
    # Elsevier tweak
    if "elsevier.com/retrieve/pii/" in url:
        url = fix_elsevier(url)

    verify_tls = urlparse(url).hostname not in BAD_TLS
    dbg(f"  DOWNLOAD {url}")
    try:
        r = SESSION.get(url, headers=extra_hdr, timeout=40,
                        allow_redirects=True, verify=verify_tls)
        dbg(f"    ↪ {r.status_code}  {r.reason}  ct={r.headers.get('Content-Type')}  len={len(r.content)}")
        r.raise_for_status()

        if "pdf" not in r.headers.get("Content-Type", "").lower():
            raise ValueError("not PDF")

        dest.write_bytes(r.content)
        return True
    except (requests.RequestException, ValueError) as e:
        dbg(f"    ! download error: {e}")
        return False

# HTML saving functionality removed - only saving PDFs now

# ─────────────────────────────── main ───────────────────────────────────────
def main():
    if not os.path.exists(STATS_CSV):
        with open(STATS_CSV, "w", newline="", encoding="utf-8") as fh:
            csv.DictWriter(fh, fieldnames=FIELDNAMES).writeheader()


    with open(CSV_IN, newline='', encoding='utf-8') as fh:
        rows = [row for row in csv.DictReader(fh)]

    # start_idx = 1200   # <-- minimal edit, 0-based (row 649 is the 650th row)
    for idx, row in enumerate(rows):
        # if idx < start_idx:
        #     continue        
        title   = row.get("title") or "untitled"
        doi_raw = row.get("doi") or ""
        doi     = sanitize_doi(doi_raw)
        pdf_url= row.get("pdf_url") or ""
        html_url= row.get("landing_url") or ""

        if "peer review" in title.lower():
            print("\nThis is a peer review, skipping\n")
            continue

        tag = f"row{idx:03d}"

        oa_flag = row.get("oa_status")
        stats_row = {                    
            "tag": tag,
            "doi": doi or "",
            "oa_status": oa_flag,
            "elsevier_error_code": "",     # HTTP code or short label
            "elsevier_pages": "",         # int, blank if not tried / failed
            "elsevier_status": "",      
            "success": 0,
            "unpaywall_status": "",
            "semantic_status": "",
            "openalex_status": "",
            "core_status": "",
            "doi_head_status": "",
            "direct_download_status": "",
        }


        dbg(f"\n=== [{tag}]  {title[:70]}")

        if not doi:
            dbg("! no DOI → skipped")
            stats_row["elsevier_error_code"] = "NO DOI, SKIPPING"
            write_stats(stats_row)
            continue

        pdf_name = f"{tag}__{safe_filename(title)}.pdf"
        elsevier_pdf_path = ELSEVIER_PDF_DIR / pdf_name
        pdf_path = PDF_DIR / pdf_name

        if elsevier_pdf_path.exists():
            dbg(f"✓ PDF already exists (elsevier) ({pdf_name})")
            continue
        if pdf_path.exists():
            dbg(f"✓ PDF already exists ({pdf_name})")
            continue



        api_url = f"https://api.elsevier.com/content/article/doi/{doi}"
        print("api_url")
        print(api_url)
        try:
            resp = requests.get(api_url, headers=get_elsevier_headers(), timeout=60)
            stats_row["elsevier_error_code"] = resp.status_code    # <- NEW
            stats_row["elsevier_status"] = resp.headers.get("X-ELS-Status", "")
            print("X-ELS-Status")
            print(resp.headers.get("X-ELS-Status", ""))
        except Exception as e:
            stats_row["elsevier_error_code"] = f"EXC:{e.__class__.__name__}"   # <- NEW
            print(f"except: {e}")
            print(f"FAIL elsevier")
            # continue
        if resp and resp.ok and looks_like_pdf(resp.content):
            elsevier_pdf_path.write_bytes(resp.content)
            num_pages = len(fitz.open(elsevier_pdf_path))            # <- NEW
            stats_row["elsevier_pages"] = num_pages                  # <- NEW
            print()
            print("ELSEVIERresp.headers")
            pprint.pprint(resp.headers)
            print(f"✅ {elsevier_pdf_path.name}   ")
            print()

            if is_full_article(str(elsevier_pdf_path)):
                stats_row["success"] = 1
                print("full article, copying...")
                dst = PDF_DIR / pdf_path.name
                shutil.copy2(elsevier_pdf_path, dst)
                stats_row["elsevier_error_code"] = "SUCCESS"        # clear it – call was a success
                write_stats(stats_row)   # see helper below
                print()
                print()
                print()
            continue
        print()

        print("elsevier failed, moving on...")

        if pdf_url:
            if download(pdf_url, pdf_path, html_ref=html_url):
                stats_row["openalex_status"] = "SUCCESS"        # clear it – call was a success
                stats_row["success"] = 1                    # <---- add this!
                write_stats(stats_row)
                dbg(f"✓ PDF saved from openalex pdf url → {pdf_name}")
                continue

        pdf_url = None
        for colname, locator in LOCATORS:
            dbg(f"* locator: {locator.__name__[4:]}")
            try:
                url = locator(doi)
                if url:
                    stats_row[colname] = "SUCCESS"
                    pdf_url = url
                    dbg(f"  → {pdf_url}")
                    break
                else:
                    stats_row[colname] = "none"
            except NameResolutionError:
                stats_row[colname] = "dns_error"
                dbg("  ! DNS error, retrying once …")
                time.sleep(3)
                try:
                    url = locator(doi)
                    if url:
                        stats_row[colname] = "SUCCESS"
                        pdf_url = url
                        dbg(f"  → {pdf_url}")
                        break
                    else:
                        stats_row[colname] = "none"
                except Exception as e:
                    stats_row[colname] = f"error:{e.__class__.__name__}"
            except Exception as e:
                stats_row[colname] = f"error:{e.__class__.__name__}"

        # -------- try to fetch ----------
        if pdf_url:
            if download(pdf_url, pdf_path, html_ref=html_url):
                stats_row["direct_download_status"] = "SUCCESS"
                stats_row["success"] = 1                  
                write_stats(stats_row)
                dbg(f"✓ PDF saved → {pdf_name}")
                continue
            else:
                stats_row["direct_download_status"] = "DIRECT DOWNLOAD FAILED"
        else:
            stats_row["direct_download_status"] = "NO PDF URL FOUND"


        dbg("– no PDF captured")
        write_stats(stats_row)
        time.sleep(1)          # steady-state politeness delay

if __name__ == "__main__":
    main()
