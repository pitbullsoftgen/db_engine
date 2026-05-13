# Ultra DB BIGDATA research Cleaner & Converter (GUI)
can open billions of line in one click on low ram low configured computer

A lightweight desktop tool for cleaning, converting, splitting, opening chunks, and merging very large CSV/JSON/TXT/Excel files with low RAM usage.

## Features

- Single file clean + convert (`csv`, `jsonl`, `txt`, `xlsx`)
- Batch directory convert with parallel worker mode (SSD-friendly)
- Big file opener tab with chunk preview (open partial rows only)
- Split tab: split one large file into many smaller parts by row count
- Merge tab: merge many files from folder or manual selection into one output file
- True streaming JSON array parser using `ijson` for ultra-large `.json` arrays
- Global pause/resume control for long jobs
- Live progress bar + ETA + job stats
- Modernized logical UI layout inspired by professional large-file editors
- Streaming-style processing for low-memory operation

## Supported Inputs

- `.csv`
- `.txt` (tab-delimited)
- `.json`
- `.jsonl`
- `.xlsx`

## Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run

```powershell
python app.py
```

## Notes For Huge Files

- For very large datasets, `csv` and `jsonl` are ideal, and large `.json` arrays now stream through `ijson`.
- Excel format is supported, but for ultra-large files, CSV/JSONL is faster and lighter.
- Use **Big File Opener (Chunk)** tab to inspect rows without loading full file.
- Use **Splitter** tab for breaking large files into chunks for easier handling.

## Long Job Controls

- `Accurate ETA (pre-scan rows)`: when enabled, ETA is more accurate but job starts slightly later.
- `Pause` and `Resume`: pause all active streaming work safely.
- Batch tab `Parallel workers`: use more workers for faster SSD throughput.

## Typical Workflow

1. Open a source file or folder.
2. Choose clean options (trim spaces, drop empty rows).
3. Choose output format and destination.
4. Process data.
5. Optionally split or merge outputs.

