#!/usr/bin/env python3
"""
archive_create.py — Create a verified, indexed archive.

Workflow:
  1. Validates all input files (exist, non-empty)
  2. Computes SHA-256 checksums for every file
  3. Creates a compressed tarball (.tar.gz)
  4. Verifies the tarball contents match what was added
  5. Writes a human-readable README (inside tarball AND alongside it)
  6. Updates the master index (~/.archive_index.json) for future searching

Usage:
  python archive_create.py -d "BAM files project1" -f sample1.bam sample2.bam
  python archive_create.py -d "FASTQ run3" -l file_list.txt --tags project1 fastq
  python archive_create.py -d "Results proj2" -f /scratch/proj2/ -o /fortress/archives/
"""

import argparse
import hashlib
import json
import os
import re
import socket
import sys
import tarfile
import tempfile
from datetime import datetime
from pathlib import Path

DEFAULT_INDEX = os.path.expanduser("~/.archive_index.json")


# ── Utilities ────────────────────────────────────────────────────────────────

def slugify(text: str) -> str:
    """Convert a description string into a filename-safe slug (max 40 chars)."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "_", text)
    return text[:40].strip("_")


def human_size(n_bytes: int) -> str:
    """Return a human-readable file size string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n_bytes < 1024:
            return f"{n_bytes:.1f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:.1f} PB"


def compute_checksum(filepath: str, algorithm: str = "sha256") -> str:
    """Compute and return a prefixed checksum string, e.g. 'sha256:abc123...'."""
    h = hashlib.new(algorithm)
    with open(filepath, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return f"{algorithm}:{h.hexdigest()}"


def validate_file(filepath: str):
    """Return (ok: bool, error_message: str | None)."""
    p = Path(filepath)
    if not p.exists():
        return False, f"Does not exist: {filepath}"
    if not p.is_file():
        return False, f"Not a regular file: {filepath}"
    if p.stat().st_size == 0:
        return False, f"File is empty (0 bytes): {filepath}"
    return True, None


def collect_files(inputs: list) -> list:
    """Expand directories recursively; return sorted list of absolute file paths."""
    all_files = []
    for inp in inputs:
        p = Path(inp)
        if p.is_dir():
            found = sorted(str(f.resolve()) for f in p.rglob("*") if f.is_file())
            if not found:
                print(f"  WARNING: directory is empty, skipping: {inp}", file=sys.stderr)
            all_files.extend(found)
        elif p.is_file():
            all_files.append(str(p.resolve()))
        else:
            print(f"  WARNING: path not found, skipping: {inp}", file=sys.stderr)
    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for f in all_files:
        if f not in seen:
            seen.add(f)
            deduped.append(f)
    return deduped


# ── Index helpers ─────────────────────────────────────────────────────────────

def load_index(index_path: str) -> dict:
    if os.path.exists(index_path):
        with open(index_path) as fh:
            return json.load(fh)
    return {"version": "1.0", "archives": []}


def save_index(index: dict, index_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(index_path)), exist_ok=True)
    with open(index_path, "w") as fh:
        json.dump(index, fh, indent=2)


# ── Core archive logic ────────────────────────────────────────────────────────

def create_archive(
    files: list,
    description: str,
    output_dir: str = ".",
    index_path: str = DEFAULT_INDEX,
    tags: list = None,
    dry_run: bool = False,
):
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    slug = slugify(description)
    archive_stem = f"{timestamp}_{slug}"
    archive_filename = f"{archive_stem}.tar.gz"
    readme_filename = f"{archive_stem}_README.txt"

    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / archive_filename
    readme_path = output_dir / readme_filename
    tags = tags or []

    print(f"\n{'=' * 62}")
    print(f"  ARCHIVE: {archive_filename}")
    print(f"{'=' * 62}")

    # ── Step 1: Validate ─────────────────────────────────────────────────────
    print(f"\n[1/5] Validating {len(files)} file(s)...")
    file_meta = []
    errors = []

    for filepath in files:
        ok, err = validate_file(filepath)
        if not ok:
            errors.append(err)
            print(f"  ✗ {err}")
        else:
            stat = Path(filepath).stat()
            file_meta.append({
                "original_path": filepath,
                "size_bytes": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })
            print(f"  ✓ {filepath}  ({human_size(stat.st_size)})")

    if errors:
        print(f"\n✗ {len(errors)} validation error(s). Fix them before archiving. Continuing anyway")

    # ── Step 2: Checksums ─────────────────────────────────────────────────────
    print(f"\n[2/5] Computing SHA-256 checksums...")
    for fm in file_meta:
        fm["checksum"] = compute_checksum(fm["original_path"])
        short = fm["checksum"][7:23] + "..."
        print(f"  ✓ {Path(fm['original_path']).name:40s}  {short}")

    if dry_run:
        print("\n[DRY RUN] Validation passed. No archive created.")
        return None

    # ── Step 3: Build tarball ─────────────────────────────────────────────────
    print(f"\n[3/5] Building archive...")
    with tempfile.TemporaryDirectory() as tmpdir:
        # Build README text (will be embedded inside the tarball)
        lines = [
            "ARCHIVE README",
            "=" * 62,
            f"Archive Name : {archive_filename}",
            f"Created      : {now.isoformat()}",
            f"Host         : {socket.gethostname()}",
            f"Description  : {description}",
            f"Tags         : {', '.join(tags) if tags else '(none)'}",
            f"Total Files  : {len(file_meta)}",
            "",
            "FILES",
            "=" * 62,
        ]
        for i, fm in enumerate(file_meta, 1):
            lines += [
                "",
                f"[{i:03d}] {Path(fm['original_path']).name}",
                f"      Original Path : {fm['original_path']}",
                f"      Size          : {human_size(fm['size_bytes'])} ({fm['size_bytes']:,} bytes)",
                f"      Modified      : {fm['modified']}",
                f"      SHA-256       : {fm['checksum']}",
            ]
        lines += ["", "END OF README", ""]
        readme_text = "\n".join(lines)

        internal_readme = Path(tmpdir) / readme_filename
        internal_readme.write_text(readme_text)

        with tarfile.open(archive_path, "w:gz") as tar:
            # README goes in first so it's always at the top of the listing
            tar.add(str(internal_readme), arcname=readme_filename)
            for fm in file_meta:
                fp = fm["original_path"]
                # Strip leading slash so paths are relative inside the tarball
                arcname = fp.lstrip("/")
                tar.add(fp, arcname=arcname)
                fm["archived_path"] = arcname
                print(f"  + {fp}")

    print(f"  ✓ Archive written: {archive_path}")

    # ── Step 4: Verify tarball ────────────────────────────────────────────────
    print(f"\n[4/5] Verifying archive integrity...")
    archive_stat = archive_path.stat()
    archive_checksum = compute_checksum(str(archive_path))

    with tarfile.open(archive_path, "r:gz") as tar:
        members = set(tar.getnames())
        missing = []
        for fm in file_meta:
            if fm["archived_path"] not in members:
                missing.append(fm["original_path"])
            else:
                print(f"  ✓ {Path(fm['original_path']).name}")
        if missing:
            print("\n✗ Files missing from tarball — archive may be corrupt!")
            for m in missing:
                print(f"    MISSING: {m}")
            sys.exit(1)

    print(f"  ✓ All {len(file_meta)} files verified in tarball")
    print(f"  ✓ Archive size     : {human_size(archive_stat.st_size)}")
    print(f"  ✓ Archive SHA-256  : {archive_checksum[7:23]}...")

    # ── Step 5: Write README + update index ───────────────────────────────────
    print(f"\n[5/5] Writing README and updating master index...")
    readme_path.write_text(readme_text)
    print(f"  ✓ README written : {readme_path}")

    index = load_index(index_path)
    entry = {
        "archive_name": archive_filename,
        "created": now.isoformat(),
        "description": description,
        "tags": tags,
        "host": socket.gethostname(),
        "archive_location": str(archive_path),
        "archive_size_bytes": archive_stat.st_size,
        "archive_checksum": archive_checksum,
        "files": file_meta,
    }
    index["archives"].append(entry)
    save_index(index, index_path)
    print(f"  ✓ Index updated  : {index_path}")

    print(f"\n{'=' * 62}")
    print(f"  ✓  DONE")
    print(f"  Archive : {archive_path}")
    print(f"  README  : {readme_path}")
    print(f"  Files   : {len(file_meta)}")
    print(f"  Size    : {human_size(archive_stat.st_size)}")
    print(f"\n  Next steps:")
    print(f"    1. Move both files to Fortress / cloud storage")
    print(f"    2. Update location in index:")
    print(f"       python archive_manage.py --update-location {archive_filename} /new/path/")
    print(f"    3. Delete local copies once safely stored")
    print(f"{'=' * 62}\n")

    return str(archive_path)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Create a verified, indexed archive.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("-d", "--description", required=True,
                        help="Brief description of archive contents (used in filename and README)")
    parser.add_argument("-f", "--files", nargs="+", metavar="PATH",
                        help="Files or directories to archive")
    parser.add_argument("-l", "--list", metavar="FILE",
                        help="Text file with one path per line (# lines are comments)")
    parser.add_argument("-o", "--output", default=".",
                        help="Directory to write archive into (default: current dir)")
    parser.add_argument("--tags", nargs="+", metavar="TAG", default=[],
                        help="Optional tags for searching, e.g. --tags project1 bam rna-seq")
    parser.add_argument("--index", default=DEFAULT_INDEX,
                        help=f"Master index file (default: {DEFAULT_INDEX})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate files but do not create the archive")
    args = parser.parse_args()

    inputs = list(args.files or [])
    if args.list:
        list_path = Path(args.list)
        if not list_path.exists():
            print(f"Error: list file not found: {args.list}", file=sys.stderr)
            sys.exit(1)
        with open(list_path) as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#"):
                    inputs.append(line)

    if not inputs:
        parser.error("Provide files via -f and/or a list file via -l.")

    files = collect_files(inputs)
    if not files:
        print("No files found to archive.", file=sys.stderr)
        sys.exit(1)

    print(f"Collected {len(files)} file(s) to archive.")
    create_archive(files, args.description, args.output, args.index, args.tags, args.dry_run)


if __name__ == "__main__":
    main()
