#!/usr/bin/env python3

"""Remove generated .h5ad files from a Biomni output directory."""

from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_OUTPUT_DIR = Path("/cs/student/projects2/aisd/2024/shekchu/projects/agent_outputs/non_clustered/biomni")


def iter_h5ad_files(root: Path) -> list[Path]:
    if not root.exists():
        return []

    return sorted(path for path in root.rglob("*.h5ad") if path.is_file())


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove generated .h5ad files from a Biomni output tree.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Root Biomni output directory to clean",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be removed without deleting them",
    )

    args = parser.parse_args()

    h5ad_files = iter_h5ad_files(args.output_dir)
    if not h5ad_files:
        print(f"No .h5ad files found under {args.output_dir}")
        return

    action = "Would remove" if args.dry_run else "Removing"
    print(f"{action} {len(h5ad_files)} .h5ad file(s) under {args.output_dir}")

    removed = 0
    for h5ad_file in h5ad_files:
        print(h5ad_file)
        if args.dry_run:
            continue

        h5ad_file.unlink()
        removed += 1

    if not args.dry_run:
        print(f"Removed {removed} file(s)")


if __name__ == "__main__":
    main()