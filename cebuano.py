import csv
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone

import requests


# ============================================================
# CONFIGURATION
# ============================================================

CATEGORY = "Category:Lingua Libre pronunciation-ceb"
API_URL = "https://commons.wikimedia.org/w/api.php"

OUTPUT_DIR = Path("cebuano_dataset")
AUDIO_DIR = OUTPUT_DIR / "audio"
METADATA_FILE = OUTPUT_DIR / "metadata.csv"

USER_AGENT = (
    "CebuanoSpeechDatasetDownloader/2.0 "
    "(educational research project; contact: YOUR_EMAIL)"
)

# Tune these based on Wikimedia's responses.
MAX_WORKERS = 4
BATCH_SIZE = 50
CHUNK_SIZE = 1024 * 1024
MAX_RETRIES = 6
REQUEST_TIMEOUT = (15, 120)
DOWNLOAD_DELAY = 0.15

# Set to True to recheck the size of existing audio files.
VERIFY_EXISTING_SIZE = True

CSV_FIELDS = [
    "filename",
    "text",
    "speaker",
    "source",
    "license",
    "original_url",
]

_thread_local = threading.local()


# ============================================================
# HTTP SESSIONS
# One persistent session per worker thread.
# ============================================================

def get_session():
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        _thread_local.session = session

    return _thread_local.session


def retry_after_seconds(response, attempt):
    value = response.headers.get("Retry-After", "").strip()

    if value:
        try:
            return max(0, min(float(value), 120))
        except ValueError:
            try:
                retry_time = parsedate_to_datetime(value)
                if retry_time.tzinfo is None:
                    retry_time = retry_time.replace(tzinfo=timezone.utc)
                seconds = (
                    retry_time - datetime.now(timezone.utc)
                ).total_seconds()
                return max(0, min(seconds, 120))
            except (ValueError, TypeError, OverflowError):
                pass

    return min(2 ** attempt, 60)


def request_with_retry(url, *, stream=False, **kwargs):
    """Retry transient HTTP failures and network errors."""
    last_error = None

    for attempt in range(MAX_RETRIES):
        response = None

        try:
            response = get_session().get(
                url,
                stream=stream,
                timeout=REQUEST_TIMEOUT,
                **kwargs,
            )

            if response.status_code in (429, 500, 502, 503, 504):
                wait = retry_after_seconds(response, attempt)
                status = response.status_code
                response.close()

                if attempt == MAX_RETRIES - 1:
                    raise RuntimeError(
                        f"HTTP {status}; retry limit reached"
                    )

                print(
                    f"\n  HTTP {status}; retrying in {wait:.1f}s"
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response

        except (requests.RequestException, RuntimeError) as exc:
            last_error = exc

            if response is not None:
                response.close()

            if attempt == MAX_RETRIES - 1:
                break

            wait = min(2 ** attempt, 60)
            print(
                f"\n  Request failed ({attempt + 1}/{MAX_RETRIES}): "
                f"{exc}; waiting {wait}s"
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Request failed after {MAX_RETRIES} attempts: {last_error}"
    )


# ============================================================
# WIKIMEDIA CATEGORY
# ============================================================

def get_category_files():
    files = []
    continuation = {}

    print(f"Searching: {CATEGORY}")

    while True:
        params = {
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "list": "categorymembers",
            "cmtitle": CATEGORY,
            "cmnamespace": 6,
            "cmtype": "file",
            "cmlimit": "max",
        }
        params.update(continuation)

        with request_with_retry(
            API_URL, params=params, stream=False
        ) as response:
            data = response.json()

        if "error" in data:
            raise RuntimeError(f"Wikimedia API error: {data['error']}")

        for member in data.get("query", {}).get("categorymembers", []):
            title = member.get("title", "")
            if title.lower().endswith(".wav"):
                files.append(title)

        print(f"\rFound {len(files)} WAV files", end="", flush=True)

        continuation = data.get("continue")
        if not continuation:
            break

    # Avoid duplicate titles if the category data changes during pagination.
    files = list(dict.fromkeys(files))

    print(f"\nTotal unique WAV files: {len(files)}")
    return files


# ============================================================
# FILE INFORMATION IN BATCHES
# ============================================================

def get_file_infos(titles):
    params = {
        "action": "query",
        "format": "json",
        "formatversion": 2,
        "prop": "imageinfo",
        "iiprop": "url|size|mime|timestamp|extmetadata",
        "titles": "|".join(titles),
    }

    with request_with_retry(
        API_URL, params=params, stream=False
    ) as response:
        data = response.json()

    if "error" in data:
        raise RuntimeError(f"Wikimedia API error: {data['error']}")

    # API normalization maps requested titles to canonical titles.
    aliases = {}
    for item in data.get("query", {}).get("normalized", []):
        aliases[item["to"]] = item["from"]
    for item in data.get("query", {}).get("redirects", []):
        aliases[item["to"]] = item["from"]

    result = {}

    for page in data.get("query", {}).get("pages", []):
        imageinfos = page.get("imageinfo", [])
        if not imageinfos:
            continue

        info = imageinfos[0]
        metadata = info.get("extmetadata", {})
        title = page.get("title", "")
        requested_title = aliases.get(title, title)

        result[requested_title] = {
            "url": info.get("url", ""),
            "size": info.get("size", 0),
            "mime": info.get("mime", ""),
            "timestamp": info.get("timestamp", ""),
            "license": metadata.get(
                "LicenseShortName", {}
            ).get("value", "Check file page"),
            "license_url": metadata.get(
                "LicenseUrl", {}
            ).get("value", ""),
            "description_url": page.get("fullurl", ""),
        }

    # Also index by canonical title when it is already the requested title.
    return result


# ============================================================
# FILENAME PARSING
# ============================================================

def parse_filename(title):
    filename = Path(title.removeprefix("File:")).name
    stem = re.sub(r"\.wav$", "", filename, flags=re.IGNORECASE)

    match = re.match(r"^LL-[^-]+ \(ceb\)-(.+?)-(.+)$", stem)

    if match:
        return match.group(1).strip(), match.group(2).strip()

    return "", stem


# ============================================================
# DOWNLOAD
# Each worker writes to a temporary file, then atomically renames it.
# ============================================================

def download_file(title, info):
    url = info.get("url")
    if not url:
        return {"title": title, "status": "failed",
                "error": "No download URL"}

    filename = Path(title.removeprefix("File:")).name
    destination = AUDIO_DIR / filename
    temp_path = destination.with_name(destination.name + ".part")
    expected_size = int(info.get("size") or 0)

    if destination.exists():
        actual_size = destination.stat().st_size
        if (
            not VERIFY_EXISTING_SIZE
            or not expected_size
            or actual_size == expected_size
        ):
            return {
                "title": title,
                "status": "skipped",
                "filename": filename,
                "info": info,
            }

    try:
        # A stale temporary file is safe to discard: downloads restart
        # from the beginning rather than pretending to resume partial data.
        temp_path.unlink(missing_ok=True)

        with request_with_retry(url, stream=True) as response:
            content_length = response.headers.get("Content-Length")
            total = int(content_length) if content_length else expected_size
            downloaded = 0

            with temp_path.open("wb") as output:
                for chunk in response.iter_content(CHUNK_SIZE):
                    if not chunk:
                        continue
                    output.write(chunk)
                    downloaded += len(chunk)

            if expected_size and downloaded != expected_size:
                raise IOError(
                    f"Size mismatch: expected {expected_size}, "
                    f"received {downloaded}"
                )

            if total and downloaded != total:
                raise IOError(
                    f"Incomplete response: expected {total}, "
                    f"received {downloaded}"
                )

        temp_path.replace(destination)

        if DOWNLOAD_DELAY:
            time.sleep(DOWNLOAD_DELAY)

        return {
            "title": title,
            "status": "downloaded",
            "filename": filename,
            "info": info,
        }

    except Exception as exc:
        temp_path.unlink(missing_ok=True)
        return {
            "title": title,
            "status": "failed",
            "error": str(exc),
        }


# ============================================================
# METADATA
# ============================================================

def load_existing_metadata():
    rows = {}

    if not METADATA_FILE.exists():
        return rows

    with METADATA_FILE.open(
        "r", newline="", encoding="utf-8-sig"
    ) as file:
        for row in csv.DictReader(file):
            if row.get("filename"):
                rows[row["filename"]] = row

    return rows


def write_metadata(rows):
    """Rewrite atomically to prevent a half-written CSV on interruption."""
    temp_path = METADATA_FILE.with_suffix(".csv.tmp")

    with temp_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()

        for filename in sorted(rows):
            writer.writerow(rows[filename])

    temp_path.replace(METADATA_FILE)


def make_metadata(filename, title, info):
    speaker, text = parse_filename(title)

    return {
        "filename": filename,
        "text": text,
        "speaker": speaker,
        "source": "Wikimedia Commons - Lingua Libre",
        "license": info.get("license") or "Check file page",
        "original_url": info.get("url", ""),
    }


# ============================================================
# MAIN
# ============================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("CEBUANO WIKIMEDIA SPEECH DATASET DOWNLOADER")
    print(f"Concurrent workers: {MAX_WORKERS}")
    print("=" * 60)

    files = get_category_files()
    if not files:
        print("No WAV files found.")
        return

    metadata = load_existing_metadata()
    successful = 0
    failed = 0
    skipped = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for start in range(0, len(files), BATCH_SIZE):
            batch = files[start:start + BATCH_SIZE]

            try:
                infos = get_file_infos(batch)
            except Exception as exc:
                print(f"\nMetadata batch failed at {start}: {exc}")
                failed += len(batch)
                continue

            futures = {
                executor.submit(download_file, title, infos[title]): title
                for title in batch
                if title in infos
            }

            missing = [title for title in batch if title not in infos]
            for title in missing:
                print(f"\nMissing file information: {title}")
            failed += len(missing)

            for future in as_completed(futures):
                result = future.result()
                completed += 1
                title = result["title"]
                status = result["status"]

                if status in ("downloaded", "skipped"):
                    filename = result["filename"]
                    info = result["info"]

                    # Preserve previously recorded rows where possible.
                    if filename not in metadata:
                        metadata[filename] = make_metadata(
                            filename, title, info
                        )

                    if status == "downloaded":
                        successful += 1
                    else:
                        skipped += 1
                else:
                    failed += 1
                    print(
                        f"\nFAILED: {title}\n  {result.get('error', '')}"
                    )

                if completed % 10 == 0:
                    write_metadata(metadata)

                print(
                    f"\rProcessed {completed + failed}/{len(files)} | "
                    f"Downloaded: {successful} | Skipped: {skipped} | "
                    f"Failed: {failed}",
                    end="",
                    flush=True,
                )

            # Persist each completed batch, not just at program exit.
            write_metadata(metadata)

    print("\n\n" + "=" * 60)
    print("DOWNLOAD COMPLETE")
    print("=" * 60)
    print(f"Downloaded: {successful}")
    print(f"Skipped:    {skipped}")
    print(f"Failed:     {failed}")
    print(f"Audio:      {AUDIO_DIR.resolve()}")
    print(f"Metadata:   {METADATA_FILE.resolve()}")


if __name__ == "__main__":
    main()
