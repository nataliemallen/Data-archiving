#!/usr/bin/env python3
"""
archive_retrieve.py — Extract specific files from an archive with checksum verification.

Usage:
  python archive_retrieve.py 20240315_bam_project1.tar.gz --files sample1.bam
  python archive_retrieve.py 20240315_bam_project1.tar.gz --files "*.bam" -o ~/restored/
  python archive_retrieve.py 20240315_bam_project1.tar.gz --all --restore-paths
  python archive_retrieve.py 20240315_bam_project1.tar.gz --list
  python archive_retrieve.py 20240315_bam_project1.tar.gz --archive-path /fortress/arc/20240315_bam_project1.tar.gz --files sample1.bam
"""

import argparse
import hashlib
import json
import os
import sys
import tarfile
from fnmatch import fnmatch
from pathlib import Path

DEFAULT_INDEX = os.path.expanduser("~/.archive_index.json")


def human_size(n_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n_bytes < 1024:
            return f"{n_bytes:.1f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:.1f} PB"


def compute_checksum(filepath: str, algorithm: str = "sha256") -> str:
    h = hashlib.new(algorithm)
    with open(filepath, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return f"{algorithm}:{h.hexdigest()}"


def load_index(index_path: str) -> dict:
    if not os.path.exists(index_path):
        print(f"No index found at: {index_path}", file=sys.stderr)
        sys.exit(1)
    with open(index_path) as fh:
        return json.load(fh)


def find_archive_entry(archive_name: str, index: dict):
    """Return the index entry for a given archive filename."""
    name = Path(archive_name).name  # strip any path prefix for matching
    for arch in index.get("archives", []):
        if arch["archive_name"] == name:
            return arch
    return None


def resolve_archive_path(archive_name: str, archive_entry: dict, override_path: str = None) -> str:
    """Find the actual file path of the tarball."""
    if override_path:
        p = Path(override_path)
        if p.exists():
            return str(p)
        print(f"Error: supplied archive path not found: {override_path}", file=sys.stderr)
        sys.exit(1)

    # Try the location stored in the index
    if archive_entry:
        stored = archive_entry.get("archive_location", "")
        if stored and Path(stored).exists():
            return stored

    # Try the bare name in the current directory
    if Path(archive_name).exists():
        return str(Path(archive_name).resolve())

    return None


def match_files(patterns: list, file_meta: list) -> list:
    """Match a list of name/wildcard patterns against file metadata entries."""
    matched = []
    already = set()

    for pattern in patterns:
        found_any = False
        for fm in file_meta:
            orig = fm["original_path"]
            name = Path(orig).name
            # Match against bare filename first, then full path
            if fnmatch(name, pattern) or fnmatch(orig, pattern) \
               or (("*" not in pattern and "?" not in pattern) and
                   (pattern == name or pattern == orig)):
                if orig not in already:
                    matched.append(fm)
                    already.add(orig)
                    found_any = True
        if not found_any:
            print(f"  WARNING: no file matching '{pattern}' in this archive", file=sys.stderr)

    return matched


def extract_files(
    archive_path: str,
    archive_entry: dict,
    file_meta: list,
    output_dir: str,
    restore_paths: bool,
    verify: bool,
) -> None:
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nExtracting {len(file_meta)} file(s)")
    print(f"  From   : {archive_path}")
    print(f"  To     : {output_dir}")
    print()

    success = 0
    failures = []

    with tarfile.open(archive_path, "r:gz") as tar:
        for fm in file_meta:
            arcname = fm["archived_path"]
            orig_name = Path(fm["original_path"]).name

            try:
                member = tar.getmember(arcname)
            except KeyError:
                failures.append(f"Not found in tarball: {arcname}")
                print(f"  ✗ NOT IN ARCHIVE : {orig_name}")
                continue

            if restore_paths:
                out_path = output_dir / arcname
            else:
                out_path = output_dir / orig_name

            out_path.parent.mkdir(parents=True, exist_ok=True)

            with tar.extractfile(member) as src, open(out_path, "wb") as dst:
                dst.write(src.read())

            if verify:
                extracted_checksum = compute_checksum(str(out_path))
                if extracted_checksum == fm["checksum"]:
                    print(f"  ✓ {orig_name:45s}  checksum OK")
                    success += 1
                else:
                    failures.append(f"Checksum mismatch: {orig_name}")
                    print(f"  ✗ {orig_name:45s}  CHECKSUM MISMATCH")
                    print(f"      expected : {fm['checksum']}")
                    print(f"      got      : {extracted_checksum}")
            else:
                print(f"  ✓ {orig_name}")
                success += 1

    print()
    print(f"  Extracted : {success}/{len(file_meta)} file(s)")
    if failures:
        print(f"  FAILURES  : {len(failures)}")
        for f in failures:
            print(f"    • {f}")
    else:
        print(f"  All files verified ✓")
    print(f"  Output    : {output_dir}")
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extract files from an archive with checksum verification.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("archive",
                        help="Archive filename (as stored in the index) or full path")
    parser.add_argument("--files", nargs="+", metavar="PATTERN",
                        help="Filenames or wildcard patterns to extract (e.g. '*.bam')")
    parser.add_argument("--all", action="store_true",
                        help="Extract all files in the archive")
    parser.add_argument("--list", action="store_true",
                        help="List archive contents without extracting")
    parser.add_argument("-o", "--output", default=".",
                        help="Output directory (default: current dir)")
    parser.add_argument("--restore-paths", action="store_true",
                        help="Recreate original directory structure under --output")
    parser.add_argument("--no-verify", action="store_true",
                        help="Skip checksum verification after extraction")
    parser.add_argument("--archive-path", metavar="PATH",
                        help="Override the archive file location (useful when moved to Fortress)")
    parser.add_argument("--index", default=DEFAULT_INDEX,
                        help=f"Master index file (default: {DEFAULT_INDEX})")

    args = parser.parse_args()
    index = load_index(args.index)

    archive_name = Path(args.archive).name
    entry = find_archive_entry(archive_name, index)

    if entry is None:
        print(f"Error: '{archive_name}' not found in index.", file=sys.stderr)
        print("Use 'python archive_find.py --list-all' to see all known archives.", file=sys.stderr)
        print("If you have the tarball and README, you can read the README directly for", file=sys.stderr)
        print("file listings, then use --archive-path to point to the tarball.", file=sys.stderr)
        sys.exit(1)

    archive_path = resolve_archive_path(archive_name, entry, args.archive_path)

    if archive_path is None:
        print(f"Error: cannot locate tarball for '{archive_name}'.", file=sys.stderr)
        print(f"  Index says it should be at: {entry.get('archive_location', '?')}", file=sys.stderr)
        print(f"  Use --archive-path /actual/path/to/{archive_name} to specify location.", file=sys.stderr)
        sys.exit(1)

    file_meta = entry.get("files", [])

    # ── List mode ─────────────────────────────────────────────────────────────
    if args.list:
        print(f"\nContents of {archive_name}")
        print("=" * 62)
        print(f"  Description : {entry['description']}")
        print(f"  Created     : {entry['created']}")
        print(f"  Host        : {entry.get('host', '?')}")
        print(f"  Tags        : {', '.join(entry.get('tags', [])) or '(none)'}")
        print(f"  Files       : {len(file_meta)}")
        print()
        for i, fm in enumerate(file_meta, 1):
            print(f"  [{i:03d}] {fm['original_path']}")
            print(f"        {human_size(fm['size_bytes'])}  │  modified {fm['modified'][:10]}")
        print()
        return

    # ── Determine which files to extract ─────────────────────────────────────
    if args.all:
        to_extract = file_meta
    elif args.files:
        to_extract = match_files(args.files, file_meta)
    else:
        parser.error("Specify --files <pattern>, --all, or --list.")

    if not to_extract:
        print("No matching files to extract.")
        sys.exit(0)

    extract_files(
        archive_path,
        entry,
        to_extract,
        args.output,
        restore_paths=args.restore_paths,
        verify=not args.no_verify,
    )


if __name__ == "__main__":
    main()
  
