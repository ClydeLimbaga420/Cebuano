import csv
import random
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests


# ============================================================
# CONFIGURATION
# ============================================================

CATEGORY = "Category:Lingua Libre pronunciation-ceb"

OUTPUT_DIR = Path("cebuano_dataset")
AUDIO_DIR = OUTPUT_DIR / "audio"
METADATA_FILE = OUTPUT_DIR / "metadata.csv"
FAILED_FILE = OUTPUT_DIR / "failed_downloads.txt"

API_URL = "https://commons.wikimedia.org/w/api.php"

USER_AGENT = (
    "CebuanoSpeechDatasetDownloader/2.0 "
    "(educational/research project; contact: add-your-email)"
)

# Retry settings
MAX_RETRIES = 8
MAX_RATE_LIMIT_WAIT = 30 * 60  # Up to 30 minutes per rate-limited request
BASE_RETRY_DELAY = 10
MAX_RETRY_DELAY = 300

# Download settings
CHUNK_SIZE = 1024 * 1024  # 1 MB
BATCH_SIZE = 25
DOWNLOAD_DELAY = 8.0
MAX_DOWNLOAD_DELAY = 120.0
REQUEST_TIMEOUT = (30, 120)

# Avoid hammering the API after a 429.
API_DELAY = 3.0

current_delay = DOWNLOAD_DELAY
last_request_time = 0.0

AUDIO_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


# ============================================================
# RATE LIMITING AND HTTP RETRIES
# ============================================================

def pace_request():
    """Ensure requests are separated by the adaptive delay."""

    global last_request_time

    elapsed = time.monotonic() - last_request_time
    wait = current_delay - elapsed

    if wait > 0:
        time.sleep(wait)

    last_request_time = time.monotonic()


def retry_after_seconds(response):
    """Parse Retry-After as seconds or an HTTP date."""

    value = response.headers.get("Retry-After")

    if not value:
        return None

    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        retry_date = parsedate_to_datetime(value)

        if retry_date.tzinfo is None:
            retry_date = retry_date.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        return max(0.0, (retry_date - now).total_seconds())

    except (TypeError, ValueError, OverflowError):
        return None


def get_with_retry(url, **kwargs):
    """
    GET with retries for rate limits and transient server errors.

    Raises an exception if retries are exhausted. Responses returned
    by this function belong to the caller, who must close them.
    """

    global current_delay

    rate_limit_waited = 0.0
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        pace_request()

        response = None

        try:
            response = session.get(url, **kwargs)

            if response.status_code == 429:
                last_error = "HTTP 429 (Too Many Requests)"

                suggested_wait = retry_after_seconds(response)

                if suggested_wait is None:
                    suggested_wait = min(
                        BASE_RETRY_DELAY * (2 ** (attempt - 1)),
                        MAX_RETRY_DELAY,
                    )

                # Never retry sooner than the server's requested wait.
                wait = max(suggested_wait, BASE_RETRY_DELAY)
                wait += random.uniform(1, 5)

                response.close()

                if rate_limit_waited + wait > MAX_RATE_LIMIT_WAIT:
                    raise RuntimeError(
                        "Rate limit persisted for approximately "
                        f"{rate_limit_waited:.0f} seconds. "
                        "Stopping this request to avoid hammering the server."
                    )

                rate_limit_waited += wait
                current_delay = min(
                    max(current_delay * 2, DOWNLOAD_DELAY),
                    MAX_DOWNLOAD_DELAY,
                )

                print(
                    f"\n  HTTP 429. Waiting {wait:.0f}s "
                    f"(rate-limit wait: {rate_limit_waited:.0f}s; "
                    f"download delay: {current_delay:.0f}s)"
                )

                time.sleep(wait)
                continue

            if response.status_code in (500, 502, 503, 504):
                last_error = f"HTTP {response.status_code}"
                suggested_wait = retry_after_seconds(response)

                wait = suggested_wait
                if wait is None:
                    wait = min(
                        BASE_RETRY_DELAY * (2 ** (attempt - 1)),
                        MAX_RETRY_DELAY,
                    )

                wait += random.uniform(1, 4)
                response.close()

                if attempt == MAX_RETRIES:
                    break

                print(
                    f"\n  {last_error}; retrying in {wait:.0f}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError:
            if response is not None:
                response.close()
            raise

        except requests.exceptions.RequestException as exc:
            if response is not None:
                response.close()

            last_error = exc

            if attempt == MAX_RETRIES:
                break

            wait = min(
                BASE_RETRY_DELAY * attempt,
                MAX_RETRY_DELAY,
            ) + random.uniform(1, 4)

            print(
                f"\n  Network error: {exc}; retrying in {wait:.0f}s "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Request failed after retries: {url}\nReason: {last_error}"
    )


# ============================================================
# WIKIMEDIA API HELPERS
# ============================================================

def get_category_files():
    """Retrieve all WAV files from the category, including continuation."""

    files = []
    continuation = {}

    print("Searching Wikimedia Commons...")
    print(f"Category: {CATEGORY}\n")

    while True:
        params = {
            "action": "query",
            "format": "json",
            "list": "categorymembers",
            "cmtitle": CATEGORY,
            "cmnamespace": 6,
            "cmlimit": "max",
        }
        params.update(continuation)

        with get_with_retry(
            API_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        ) as response:
            data = response.json()

        if "error" in data:
            raise RuntimeError(f"Wikimedia API error: {data['error']}")

        members = data.get("query", {}).get("categorymembers", [])

        for member in members:
            title = member.get("title", "")
            if title.lower().endswith(".wav"):
                files.append(title)

        print(f"Found {len(files)} WAV files...", end="\r")

        if "continue" not in data:
            break

        continuation = data["continue"]

        # Extra courtesy delay between API pages.
        time.sleep(API_DELAY)

    # Keep order but remove duplicate titles.
    files = list(dict.fromkeys(files))

    print(f"\nTotal WAV files found: {len(files)}\n")
    return files


def get_file_infos(titles):
    """Get URLs, sizes, and metadata for a batch of file titles."""

    params = {
        "action": "query",
        "format": "json",
        "prop": "imageinfo",
        "iiprop": "url|size|mime|timestamp",
        "titles": "|".join(titles),
    }

    with get_with_retry(
        API_URL,
        params=params,
        timeout=REQUEST_TIMEOUT,
    ) as response:
        data = response.json()

    if "error" in data:
        raise RuntimeError(f"Wikimedia API error: {data['error']}")

    query = data.get("query", {})

    normalized = {
        item["to"]: item["from"]
        for item in query.get("normalized", [])
    }

    result = {}

    for page in query.get("pages", {}).values():
        imageinfo = page.get("imageinfo")

        if not imageinfo:
            continue

        info = imageinfo[0]
        title = page.get("title", "")
        original_title = normalized.get(title, title)

        result[original_title] = {
            "url": info.get("url"),
            "size": info.get("size", 0),
            "mime": info.get("mime", ""),
            "timestamp": info.get("timestamp", ""),
        }

    return result


# ============================================================
# FILENAME PARSING
# ============================================================

def parse_filename(title):
    """
    Example:
        LL-Q33239 (ceb)-Wu Xielong-Gabi.wav

    Returns:
        speaker = Wu Xielong
        text = Gabi
    """

    filename = title.removeprefix("File:")
    filename = Path(filename).name

    stem = re.sub(r"\.wav$", "", filename, flags=re.IGNORECASE)

    pattern = r"^LL-[^-]+ \(ceb\)-(.+?)-(.+)$"
    match = re.match(pattern, stem)

    if match:
        speaker = match.group(1).strip()
        text = match.group(2).strip()
    else:
        speaker = ""
        text = stem

    return speaker, text


# ============================================================
# DOWNLOAD AND RESUME SUPPORT
# ============================================================

def download_file(url, destination, expected_size=None):
    """
    Download a file safely.

    Returns:
        "skipped"    - an existing file appears complete
        "downloaded" - download completed successfully
        None         - download failed

    Partial files are retained for future resumption.
    """

    destination.parent.mkdir(parents=True, exist_ok=True)
    part_file = destination.with_name(destination.name + ".part")

    # An existing final file is valid if its size matches the API.
    if destination.exists():
        existing_size = destination.stat().st_size

        if expected_size and existing_size == expected_size:
            return "skipped"

        if not expected_size and existing_size > 0:
            # Without a known size, don't assume an existing file is bad.
            return "skipped"

        if expected_size and existing_size > expected_size:
            destination.unlink()

    # Continue an interrupted transfer from the .part file.
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        resume_from = part_file.stat().st_size if part_file.exists() else 0

        if expected_size and resume_from > expected_size:
            part_file.unlink()
            resume_from = 0

        headers = {}

        if resume_from:
            headers["Range"] = f"bytes={resume_from}-"

        print(f"Downloading: {destination.name}")

        try:
            with get_with_retry(
                url,
                stream=True,
                timeout=REQUEST_TIMEOUT,
                headers=headers,
            ) as response:

                # Resume only when the server confirms partial content.
                if resume_from and response.status_code == 206:
                    content_range = response.headers.get(
                        "Content-Range", ""
                    )
                    expected_prefix = f"bytes {resume_from}-"

                    if not content_range.startswith(expected_prefix):
                        raise RuntimeError(
                            "Server returned an unexpected Content-Range: "
                            f"{content_range!r}"
                        )

                    mode = "ab"
                    already_downloaded = resume_from

                    range_total = content_range.split("/")[-1]
                    if range_total.isdigit():
                        total = int(range_total)
                    else:
                        total = expected_size or 0

                else:
                    # The server ignored Range or returned the whole file.
                    # Start from zero and never append a full response.
                    mode = "wb"
                    already_downloaded = 0

                    content_length = response.headers.get(
                        "Content-Length", "0"
                    )

                    try:
                        total = int(content_length)
                    except ValueError:
                        total = 0

                    if not total:
                        total = expected_size or 0

                downloaded = already_downloaded

                with open(part_file, mode) as output:
                    for chunk in response.iter_content(
                        chunk_size=CHUNK_SIZE
                    ):
                        if not chunk:
                            continue

                        output.write(chunk)
                        downloaded += len(chunk)

                        if total:
                            percent = downloaded / total * 100
                            print(
                                f"\r  {percent:6.2f}% "
                                f"({downloaded / 1024 / 1024:.2f} MB)",
                                end="",
                                flush=True,
                            )

                    output.flush()

            print()

            actual_size = (
                part_file.stat().st_size if part_file.exists() else 0
            )

            if expected_size and actual_size != expected_size:
                last_error = (
                    f"Size mismatch: got {actual_size} bytes, "
                    f"expected {expected_size}"
                )
                print(f"  {last_error}")

                if attempt < MAX_RETRIES:
                    wait = min(10 * attempt, MAX_RETRY_DELAY)
                    print(f"  Retrying in {wait}s...")
                    time.sleep(wait)
                    continue

                break

            if actual_size == 0:
                last_error = "Downloaded file is empty"
                break

            # Atomic rename after a complete download.
            part_file.replace(destination)

            print(f"  Saved: {destination.name}")
            return "downloaded"

        except requests.exceptions.RequestException as exc:
            last_error = str(exc)
            partial_size = (
                part_file.stat().st_size if part_file.exists() else 0
            )

            print(
                f"\n  Connection interrupted after {partial_size} bytes: "
                f"{exc}"
            )

            if attempt < MAX_RETRIES:
                wait = min(10 * attempt, MAX_RETRY_DELAY)
                print(f"  Waiting {wait}s before retry...")
                time.sleep(wait)

        except Exception as exc:
            last_error = str(exc)
            print(f"\n  Download error: {exc}")

            # Preserve the partial file for a later run.
            break

    print(f"  Giving up on {destination.name}: {last_error}")
    return None


# ============================================================
# METADATA AND FAILED FILE TRACKING
# ============================================================

def load_existing_filenames():
    if not METADATA_FILE.exists():
        return set()

    with open(METADATA_FILE, newline="", encoding="utf-8") as file:
        return {
            row["filename"]
            for row in csv.DictReader(file)
            if row.get("filename")
        }


def load_failed_titles():
    if not FAILED_FILE.exists():
        return set()

    with open(FAILED_FILE, encoding="utf-8") as file:
        return {
            line.strip()
            for line in file
            if line.strip()
        }


def save_failed_titles(failed_titles):
    """Rewrite the retry list so it always reflects current failures."""

    temp_file = FAILED_FILE.with_suffix(".tmp")

    with open(temp_file, "w", encoding="utf-8") as file:
        for title in sorted(failed_titles):
            file.write(title + "\n")

    temp_file.replace(FAILED_FILE)


def append_metadata(writer, metadata_file, recorded, filename, title, url):
    if filename in recorded:
        return

    speaker, text = parse_filename(title)

    writer.writerow([
        filename,
        text,
        speaker,
        "Wikimedia Commons - Lingua Libre",
        "Check the license of each file on Wikimedia Commons",
        url,
    ])

    metadata_file.flush()
    recorded.add(filename)


# ============================================================
# MAIN
# ============================================================

def main():
    global current_delay

    print("=" * 65)
    print("CEBUANO WIKIMEDIA SPEECH DATASET DOWNLOADER")
    print("=" * 65)
    print(f"Audio directory: {AUDIO_DIR}")
    print(f"Metadata file:   {METADATA_FILE}")
    print(f"Failed list:     {FAILED_FILE}")
    print()

    files = get_category_files()

    if not files:
        print("No WAV files found.")
        return

    recorded = load_existing_filenames()
    previous_failures = load_failed_titles()

    # Retry previously failed files first, followed by all other files.
    # Existing completed files will be skipped.
    ordered_files = (
        [title for title in files if title in previous_failures]
        + [title for title in files if title not in previous_failures]
    )

    metadata_exists = METADATA_FILE.exists()

    with open(
        METADATA_FILE,
        "a",
        newline="",
        encoding="utf-8",
    ) as metadata_file:

        writer = csv.writer(metadata_file)

        if not metadata_exists or METADATA_FILE.stat().st_size == 0:
            writer.writerow([
                "filename",
                "text",
                "speaker",
                "source",
                "license",
                "original_url",
            ])
            metadata_file.flush()

        successful = 0
        skipped = 0
        failed_titles = set()
        total_files = len(ordered_files)

        for start in range(0, total_files, BATCH_SIZE):
            batch = ordered_files[start:start + BATCH_SIZE]

            try:
                infos = get_file_infos(batch)

            except Exception as exc:
                print(
                    f"\nCould not get information for batch "
                    f"starting at {start + 1}: {exc}"
                )

                failed_titles.update(batch)
                save_failed_titles(failed_titles)

                # Stop rather than hammering the API if batch lookup fails.
                print(
                    "Batch lookup failed. Stopping this run safely. "
                    "Run the script again later to resume."
                )
                break

            for offset, title in enumerate(batch):
                index = start + offset + 1

                print()
                print(f"[{index}/{total_files}] {title}")

                info = infos.get(title)

                if not info or not info.get("url"):
                    print("  File information or URL unavailable.")
                    failed_titles.add(title)
                    save_failed_titles(failed_titles)
                    continue

                filename = Path(
                    title.removeprefix("File:")
                ).name

                destination = AUDIO_DIR / filename

                result = download_file(
                    info["url"],
                    destination,
                    info.get("size"),
                )

                if result is None:
                    failed_titles.add(title)
                    save_failed_titles(failed_titles)
                    continue

                if result == "skipped":
                    skipped += 1
                else:
                    successful += 1

                append_metadata(
                    writer,
                    metadata_file,
                    recorded,
                    filename,
                    title,
                    info["url"],
                )

                # Remove a file from the failure list after success.
                failed_titles.discard(title)
                save_failed_titles(failed_titles)

                # Pace successful downloads. Rate-limit responses also
                # increase this delay automatically.
                if result == "downloaded":
                    time.sleep(
                        current_delay + random.uniform(0, 3)
                    )

    print()
    print("=" * 65)
    print("DOWNLOAD RUN FINISHED")
    print("=" * 65)
    print(f"New downloads: {successful}")
    print(f"Already complete: {skipped}")
    print(f"Failed this run: {len(failed_titles)}")
    print(f"Remaining failed list: {FAILED_FILE}")
    print(f"Audio directory: {AUDIO_DIR}")
    print(f"Metadata file: {METADATA_FILE}")
    print()
    print("You can run the script again later to retry failed files.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user. Partial downloads are retained.")
    finally:
        session.close()
