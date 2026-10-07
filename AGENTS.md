# AGENTS.md

Single-script project: `cebuano.py` downloads Cebuano (`ceb`) Lingua Libre WAV pronunciations from Wikimedia Commons into a speech dataset. No manifest, tests, lint config, CI, or git repo.

## Run
- `python cebuano.py` (only third-party dependency: `requests`; none is pinned anywhere).
- Output paths are relative to the **current working directory**, so run from the repo root. Otherwise a stray `cebuano_dataset/` is created elsewhere. The top-level `audio/` directory is not used by the script. Output goes to `cebuano_dataset/audio/` and `cebuano_dataset/metadata.csv`.
- It hits the live Wikimedia API (network required) and downloads ~626 files, so a full run is slow. To test, monkeypatch `get_category_files` to return a slice, e.g. `real=c.get_category_files; c.get_category_files=lambda: real()[:4]; c.main()`.

## Gotchas
- Importing the module has side effects. It creates `cebuano_dataset/audio/` and the `requests.Session` at import time, so import-based testing creates directories.
- Never use the raw Commons title (`File:...`) as a local filename. On Windows `:` creates an NTFS alternate data stream, so the audio is hidden inside a file called `File`. The script strips the prefix.
- Wikimedia rate-limits (HTTP 429) aggressive clients. All requests go through `get_with_retry`, which honors `Retry-After` and can wait up to 120s. imageinfo is batched (`BATCH_SIZE`=50) and downloads are throttled by `DOWNLOAD_DELAY` (3s), which doubles up to `MAX_DELAY` after each 429. A "hang" while testing is usually a 429 wait.
- Resume behaviour: `download_file` skips files already on disk with a matching size. `main()` only appends a `metadata.csv` row if the filename isn't already recorded, so re-runs are safe.
- `metadata.csv` columns: `filename,text,speaker,source,license,original_url`. `source` and `license` are hardcoded (`CC BY-SA 4.0`). Preserve the columns, since downstream dataset consumers may depend on them.
- `parse_filename` expects `LL-Q<id> (ceb)-<speaker>-<text>.wav`. The regex is non-greedy on the speaker, so speaker names containing `-` get split wrongly. Unmatched names fall back to `speaker=""` and the whole stem as `text`.
- Only `.wav` titles in the File namespace (6) are fetched. Config (category, `MAX_RETRIES`, `USER_AGENT`) lives in constants at the top of the file.
