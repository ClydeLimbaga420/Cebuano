import csv
import re
import time
from pathlib import Path
from urllib.parse import quote

import requests


# ============================================================
# CONFIGURATION
# ============================================================

CATEGORY = "Category:Lingua Libre pronunciation-ceb"

OUTPUT_DIR = Path("cebuano_dataset")
AUDIO_DIR = OUTPUT_DIR / "audio"
METADATA_FILE = OUTPUT_DIR / "metadata.csv"

API_URL = "https://commons.wikimedia.org/w/api.php"

USER_AGENT = (
    "CebuanoSpeechDatasetDownloader/1.0 "
    "(educational/research project)"
)

MAX_RETRIES = 6
CHUNK_SIZE = 1024 * 1024  # 1 MB
BATCH_SIZE = 50           # titles per imageinfo request (anonymous API limit)
DOWNLOAD_DELAY = 3.0      # starting seconds between downloads
MAX_DELAY = 30.0          # adaptive delay ceiling after repeated 429s

current_delay = DOWNLOAD_DELAY


# ============================================================
# SETUP
# ============================================================

AUDIO_DIR.mkdir(parents=True, exist_ok=True)

session = requests.Session()
session.headers.update({
    "User-Agent": USER_AGENT
})


# ============================================================
# HTTP WITH RATE-LIMIT HANDLING
# ============================================================

def get_with_retry(url, **kwargs):
    """
    GET with retries. Honors Retry-After on 429/5xx and backs off on
    other transient errors. Caller must close the response if stream=True.
    """

    global current_delay

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = session.get(url, **kwargs)

            if response.status_code in (429, 500, 502, 503, 504):
                last_error = f"HTTP {response.status_code}"

                if response.status_code == 429:
                    # Throttled: slow down all later downloads too
                    current_delay = min(current_delay * 2, MAX_DELAY)

                try:
                    wait = float(response.headers.get("Retry-After", ""))
                except ValueError:
                    wait = 5 * 2 ** (attempt - 1)

                wait = min(wait, 120)
                response.close()

                print(
                    f"\n  {last_error}, waiting {wait:.0f}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response

        except requests.RequestException as e:
            last_error = e
            print(
                f"\n  Request error "
                f"(attempt {attempt}/{MAX_RETRIES}): {e}"
            )
            time.sleep(2 * attempt)

    raise RuntimeError(f"Giving up on {url}: {last_error}")


# ============================================================
# GET FILES FROM WIKIMEDIA CATEGORY
# ============================================================

def get_category_files():
    """
    Get every file in the Wikimedia Commons category.
    Handles API continuation automatically.
    """

    files = []
    continuation = {}

    print("Searching Wikimedia Commons...")
    print(f"Category: {CATEGORY}")
    print()

    while True:

        params = {
            "action": "query",
            "format": "json",
            "list": "categorymembers",
            "cmtitle": CATEGORY,
            "cmnamespace": 6,       # File namespace
            "cmlimit": "max",
        }

        params.update(continuation)

        response = get_with_retry(
            API_URL,
            params=params,
            timeout=30
        )

        data = response.json()

        members = data.get("query", {}).get("categorymembers", [])

        for member in members:
            title = member.get("title", "")

            # We only want WAV files
            if title.lower().endswith(".wav"):
                files.append(title)

        print(f"Found {len(files)} WAV files...", end="\r")

        if "continue" not in data:
            break

        continuation = data["continue"]

    print()
    print(f"Total WAV files found: {len(files)}")
    print()

    return files


# ============================================================
# GET ORIGINAL FILE INFORMATION
# ============================================================

def get_file_infos(titles):
    """
    Ask Wikimedia for original file URLs for many titles in one request.
    Returns {title: info}. Files without imageinfo are omitted.
    """

    params = {
        "action": "query",
        "format": "json",
        "prop": "imageinfo",
        "iiprop": "url|size|mime|timestamp",
        "titles": "|".join(titles),
    }

    response = get_with_retry(
        API_URL,
        params=params,
        timeout=30
    )

    query = response.json().get("query", {})

    # Map normalized titles back to the ones we asked for
    normalized = {
        n["to"]: n["from"] for n in query.get("normalized", [])
    }

    result = {}

    for page in query.get("pages", {}).values():

        imageinfo = page.get("imageinfo")

        if not imageinfo:
            continue

        info = imageinfo[0]
        title = page.get("title", "")
        title = normalized.get(title, title)

        result[title] = {
            "url": info.get("url"),
            "size": info.get("size", 0),
            "mime": info.get("mime", ""),
            "timestamp": info.get("timestamp", ""),
        }

    return result


# ============================================================
# PARSE FILENAME
# ============================================================

def parse_filename(title):
    """
    Example:

    LL-Q33239 (ceb)-Wu Xielong-Gabi.wav

    becomes:

    speaker = Wu Xielong
    text = Gabi
    """

    filename = title

    if filename.startswith("File:"):
        filename = filename[5:]

    filename = Path(filename).name

    # Remove extension
    filename_without_ext = re.sub(
        r"\.wav$",
        "",
        filename,
        flags=re.IGNORECASE
    )

    # Expected format:
    #
    # LL-Q33239 (ceb)-SPEAKER-TEXT
    #
    pattern = r"^LL-[^-]+ \(ceb\)-(.+?)-(.+)$"

    match = re.match(
        pattern,
        filename_without_ext
    )

    if match:

        speaker = match.group(1).strip()
        text = match.group(2).strip()

    else:

        speaker = ""
        text = filename_without_ext

    return speaker, text


# ============================================================
# DOWNLOAD FILE
# ============================================================

def download_file(url, destination, expected_size=None):
    """
    Download url to destination. Returns "skipped", "downloaded" or None.
    """

    if destination.exists():

        if not expected_size:
            return "skipped"

        if destination.stat().st_size == expected_size:
            return "skipped"

    print(f"Downloading: {destination.name}")

    try:
        response = get_with_retry(
            url,
            stream=True,
            timeout=60
        )

        with response:

            total = int(
                response.headers.get(
                    "content-length",
                    expected_size or 0
                )
            )

            downloaded = 0

            with open(destination, "wb") as f:

                for chunk in response.iter_content(
                    chunk_size=CHUNK_SIZE
                ):

                    if not chunk:
                        continue

                    f.write(chunk)
                    downloaded += len(chunk)

                    if total:
                        percent = (downloaded / total) * 100

                        print(
                            f"\r  {percent:6.2f}% "
                            f"({downloaded / 1024 / 1024:.2f} MB)",
                            end=""
                        )

            print()

        return "downloaded"

    except Exception as e:

        print(f"\nDownload failed: {e}")

        if destination.exists():
            destination.unlink()

        return None


# ============================================================
# MAIN
# ============================================================

def load_existing_filenames():
    """
    Filenames already recorded in metadata.csv, so re-runs don't
    append duplicate rows.
    """

    if not METADATA_FILE.exists():
        return set()

    with open(METADATA_FILE, newline="", encoding="utf-8") as f:
        return {row["filename"] for row in csv.DictReader(f)}


def main():

    print("=" * 60)
    print("CEBUANO WIKIMEDIA SPEECH DATASET DOWNLOADER")
    print("=" * 60)
    print()

    files = get_category_files()

    if not files:
        print("No WAV files found.")
        return

    recorded = load_existing_filenames()
    metadata_exists = METADATA_FILE.exists()

    metadata_file = open(
        METADATA_FILE,
        "a",
        newline="",
        encoding="utf-8"
    )

    writer = csv.writer(metadata_file)

    if not metadata_exists:

        writer.writerow([
            "filename",
            "text",
            "speaker",
            "source",
            "license",
            "original_url"
        ])

    successful = 0
    failed = 0
    index = 0

    for start in range(0, len(files), BATCH_SIZE):

        batch = files[start:start + BATCH_SIZE]

        try:
            infos = get_file_infos(batch)
        except Exception as e:
            print(f"\nCould not get info for batch at {start}: {e}")
            failed += len(batch)
            index += len(batch)
            continue

        for title in batch:

            index += 1

            print()
            print(f"[{index}/{len(files)}] {title}")

            info = infos.get(title)

            if not info or not info["url"]:
                print("  Could not get file information.")
                failed += 1
                continue

            url = info["url"]

            # Preserve Wikimedia filename
            # (strip "File:" -- ":" on Windows creates an NTFS stream)
            filename = Path(title.removeprefix("File:")).name
            destination = AUDIO_DIR / filename

            result = download_file(
                url,
                destination,
                info.get("size")
            )

            if not result:
                failed += 1
                continue

            if filename not in recorded:

                speaker, text = parse_filename(title)

                writer.writerow([
                    filename,
                    text,
                    speaker,
                    "Wikimedia Commons - Lingua Libre",
                    "CC BY-SA 4.0",
                    url
                ])

                metadata_file.flush()
                recorded.add(filename)

            successful += 1

            if result == "downloaded":
                time.sleep(current_delay)

    metadata_file.close()

    print()
    print("=" * 60)
    print("DOWNLOAD COMPLETE")
    print("=" * 60)

    print(f"Successful: {successful}")
    print(f"Failed:     {failed}")
    print()

    print(f"Audio:      {AUDIO_DIR}")
    print(f"Metadata:   {METADATA_FILE}")
    print()


if __name__ == "__main__":
    main()
