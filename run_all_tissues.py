#!/usr/bin/env python3
"""
Batch runner for Biomni over all tissue h5ad files.

This script:
1. Finds all *.h5ad files under data/<tissue>/<h5ad_subdir>/
2. Runs Biomni annotation prompt for each file
3. Saves answer + metrics (+ optional log) to a JSON file per dataset
4. Cleans up generated *.h5ad files from each run directory
5. Writes a batch summary JSON
"""

from __future__ import annotations

import argparse
import json
import os
import re
import traceback
from datetime import datetime
from pathlib import Path

from biomni.agent import A1

DEFAULT_DATA_DIR = Path("/cs/student/projects2/aisd/2024/shekchu/projects/data")
DEFAULT_OUTPUT_DIR = Path("/cs/student/projects2/aisd/2024/shekchu/projects/agent_outputs/clustered/biomni")
DEFAULT_H5AD_SUBDIR = "h5ad_unlabelled_clustered"
DEFAULT_MODEL = "claude-sonnet-4-5" #claude-sonnet-4-20250514 claude-sonnet-4-5
DEFAULT_AGENT_DATA_PATH = "./data"
DEFAULT_ANTHROPIC_KEY_JSON = Path("./claude_api_key.json")

PROMPT_TEMPLATE = (
    "Perform cell type annotation on the {tissue} cancer scRNA-seq dataset at {h5ad_path}, using the clustering information in adata.obs[\"cluster\"]."
    "Save the results to {output_csv_path} as a CSV."
)


def sanitize_name(text: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9._-]+", "_", text)
    return value.strip("_")


def load_anthropic_api_key_from_json(json_path: Path | None) -> str | None:
    if json_path is None or not json_path.exists():
        return None

    data = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Claude key JSON must contain an object: {json_path}")

    for key_name in (
        "ANTHROPIC_API_KEY",
        "anthropic_api_key",
        "claude_api_key",
        "api_key",
        "key",
    ):
        value = data.get(key_name)
        if isinstance(value, str) and value.strip():
            return value.strip()

    raise ValueError(
        f"No Claude API key field found in {json_path}. Expected one of: ANTHROPIC_API_KEY, anthropic_api_key, claude_api_key, api_key, key"
    )


def find_h5ad_files(data_dir: Path, h5ad_subdir: str, tissues: list[str] | None) -> list[tuple[str, Path]]:
    targets: list[tuple[str, Path]] = []

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    requested = set(tissues) if tissues else None

    for tissue_dir in sorted(data_dir.iterdir()):
        if not tissue_dir.is_dir():
            continue

        tissue = tissue_dir.name
        if requested is not None and tissue not in requested:
            continue

        subdir = tissue_dir / h5ad_subdir
        if not subdir.exists() or not subdir.is_dir():
            continue

        for h5ad in sorted(subdir.glob("*.h5ad")):
            targets.append((tissue, h5ad))

    return targets


def build_output_run_dir(output_dir: Path, tissue: str, h5ad_path: Path) -> Path:
    dataset = sanitize_name(h5ad_path.stem)
    tissue_clean = sanitize_name(tissue)
    run_name = f"{tissue_clean}_{dataset}"
    return output_dir / run_name


def build_output_json_path(output_dir: Path, tissue: str, h5ad_path: Path) -> Path:
    run_dir = build_output_run_dir(output_dir, tissue, h5ad_path)
    return run_dir / "biomni_output.json"


def build_output_csv_path(output_dir: Path, tissue: str, h5ad_path: Path) -> Path:
    run_dir = build_output_run_dir(output_dir, tissue, h5ad_path)
    return run_dir / "cell_type_annotations.csv"


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def cleanup_h5ad_files(run_dir: Path) -> list[str]:
    removed_files: list[str] = []

    if not run_dir.exists():
        return removed_files

    for h5ad_file in sorted(run_dir.rglob("*.h5ad")):
        if not h5ad_file.is_file():
            continue

        try:
            h5ad_file.unlink()
            removed_files.append(str(h5ad_file))
        except OSError as exc:
            print(f"Warning: failed to remove {h5ad_file}: {exc}")

    return removed_files


def extract_run_metrics(agent: A1) -> dict:
    get_metrics = getattr(agent, "get_last_run_metrics", None)
    if callable(get_metrics):
        metrics = get_metrics()
    else:
        metrics = getattr(agent, "last_run_metrics", None)

    if not isinstance(metrics, dict):
        return {}

    result: dict = {}
    token_usage = metrics.get("token_usage")
    cost_usd = metrics.get("cost_usd")
    llm_calls = metrics.get("llm_calls")
    model = metrics.get("model")

    if isinstance(token_usage, dict):
        result["token_usage"] = token_usage
    if isinstance(cost_usd, dict):
        result["cost_usd"] = cost_usd
    if isinstance(llm_calls, int):
        result["llm_calls"] = llm_calls
    if model is not None:
        result["model"] = model

    return result


def run_one(
    agent: A1,
    tissue: str,
    h5ad_path: Path,
    run_dir: Path,
    output_path: Path,
    output_csv_path: Path,
    include_log: bool,
    prompt_template: str,
    dry_run: bool,
) -> dict:
    start = datetime.now().isoformat()
    prompt = prompt_template.format(
        h5ad_path=str(h5ad_path),
        tissue=tissue,
        output_dir=str(run_dir),
        output_csv_path=str(output_csv_path),
    )

    if dry_run:
        return {
            "tissue": tissue,
            "h5ad_path": str(h5ad_path),
            "run_dir": str(run_dir),
            "output_json": str(output_path),
            "output_csv": str(output_csv_path),
            "status": "dry-run",
            "success": True,
            "start_time": start,
            "end_time": datetime.now().isoformat(),
            "prompt": prompt,
            "log_entries": 0,
            "answer": None,
        }

    run_dir.mkdir(parents=True, exist_ok=True)

    # Reset the graph/checkpoint state for isolation between files.
    agent.configure()
    log, answer = agent.go(prompt)
    saved_path = agent.save_last_run_json(str(output_path), include_log=include_log)

    log_entries = len(log) if hasattr(log, "__len__") else None
    metric_fields = extract_run_metrics(agent)

    run_result = {
        "tissue": tissue,
        "h5ad_path": str(h5ad_path),
        "run_dir": str(run_dir),
        "output_json": saved_path,
        "output_csv": str(output_csv_path),
        "status": "completed",
        "success": True,
        "start_time": start,
        "end_time": datetime.now().isoformat(),
        "prompt": prompt,
        "log_entries": log_entries,
        "answer": answer,
    }

    run_result.update(metric_fields)
    return run_result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Biomni across all tissue h5ad files and save JSON outputs.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Root data directory")
    parser.add_argument("--h5ad-subdir", default=DEFAULT_H5AD_SUBDIR, help="Per-tissue subdir containing .h5ad files")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for JSON files")
    parser.add_argument("--tissues", nargs="+", default=None, help="Optional subset of tissues")
    parser.add_argument("--llm", default=DEFAULT_MODEL, help="LLM model name for Biomni")
    parser.add_argument("--agent-data-path", default=DEFAULT_AGENT_DATA_PATH, help="Biomni data path")
    parser.add_argument(
        "--anthropic-key-json",
        type=Path,
        default=DEFAULT_ANTHROPIC_KEY_JSON,
        help="Path to JSON file containing the Claude/Anthropic API key",
    )
    parser.add_argument(
        "--skip-datalake-download",
        action="store_true",
        help="Pass expected_data_lake_files=[] when creating agent",
    )
    parser.add_argument("--skip-existing", action="store_true", help="Skip files with existing output JSON")
    parser.add_argument("--dry-run", action="store_true", help="Show planned runs without executing")
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Do not include full run log in each output JSON",
    )
    parser.add_argument(
        "--prompt-template",
        default=PROMPT_TEMPLATE,
        help="Prompt template with {h5ad_path}, {tissue}, {output_dir}, and {output_csv_path} placeholders",
    )

    args = parser.parse_args()

    all_files = find_h5ad_files(args.data_dir, args.h5ad_subdir, args.tissues)
    if not all_files:
        print("No .h5ad files found for the given filters.")
        return

    print(f"Found {len(all_files)} file(s) under {args.data_dir} using subdir '{args.h5ad_subdir}'.")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    init_kwargs = {
        "path": args.agent_data_path,
        "llm": args.llm,
    }
    if args.skip_datalake_download:
        init_kwargs["expected_data_lake_files"] = []

    anthropic_api_key = load_anthropic_api_key_from_json(args.anthropic_key_json)
    if anthropic_api_key:
        os.environ["ANTHROPIC_API_KEY"] = anthropic_api_key
        print(f"Loaded ANTHROPIC_API_KEY from {args.anthropic_key_json}")
    elif not os.environ.get("ANTHROPIC_API_KEY"):
        print(
            "Warning: no Claude API key JSON found and ANTHROPIC_API_KEY is not set. "
            "Claude runs will fail unless you set one of them."
        )

    agent = A1(**init_kwargs)

    results: list[dict] = []
    include_log = not args.no_log

    for idx, (tissue, h5ad_path) in enumerate(all_files, start=1):
        run_dir = build_output_run_dir(args.output_dir, tissue, h5ad_path)
        output_json = build_output_json_path(args.output_dir, tissue, h5ad_path)
        output_csv = build_output_csv_path(args.output_dir, tissue, h5ad_path)

        print("\n" + "=" * 72)
        print(f"[{idx}/{len(all_files)}] {tissue} :: {h5ad_path.name}")
        print(f"Run dir: {run_dir}")
        print(f"Output: {output_json}")
        print(f"CSV: {output_csv}")
        print("=" * 72)

        if args.skip_existing and output_json.exists():
            run_dir.mkdir(parents=True, exist_ok=True)
            print("Skipping existing output.")
            cleaned_h5ad_files = cleanup_h5ad_files(run_dir)
            run_result = {
                "tissue": tissue,
                "h5ad_path": str(h5ad_path),
                "run_dir": str(run_dir),
                "output_json": str(output_json),
                "output_csv": str(output_csv),
                "status": "skipped",
                "success": True,
                "start_time": None,
                "end_time": datetime.now().isoformat(),
                "cleaned_h5ad_files": cleaned_h5ad_files,
            }
            write_json(run_dir / "run_summary.json", run_result)
            results.append(run_result)
            continue

        try:
            run_result = run_one(
                agent=agent,
                tissue=tissue,
                h5ad_path=h5ad_path,
                run_dir=run_dir,
                output_path=output_json,
                output_csv_path=output_csv,
                include_log=include_log,
                prompt_template=args.prompt_template,
                dry_run=args.dry_run,
            )
            cleaned_h5ad_files = cleanup_h5ad_files(run_dir)
            run_result["cleaned_h5ad_files"] = cleaned_h5ad_files
            print(f"Success: {run_result['status']}")
            if run_result.get("log_entries") is not None:
                print(f"Log entries: {run_result['log_entries']}")
            write_json(run_dir / "run_summary.json", run_result)
            results.append(run_result)
        except Exception as exc:
            print(f"Failed: {exc}")
            run_dir.mkdir(parents=True, exist_ok=True)
            tb_text = traceback.format_exc()
            print(tb_text)
            (run_dir / "error_traceback.txt").write_text(tb_text, encoding="utf-8")
            metric_fields = extract_run_metrics(agent)
            run_result = {
                "tissue": tissue,
                "h5ad_path": str(h5ad_path),
                "run_dir": str(run_dir),
                "output_json": str(output_json),
                "output_csv": str(output_csv),
                "status": "failed",
                "success": False,
                "error": str(exc),
                "start_time": datetime.now().isoformat(),
                "end_time": datetime.now().isoformat(),
            }
            run_result.update(metric_fields)
            cleaned_h5ad_files = cleanup_h5ad_files(run_dir)
            run_result["cleaned_h5ad_files"] = cleaned_h5ad_files
            write_json(run_dir / "run_summary.json", run_result)
            results.append(run_result)

    summary = {
        "started_at": results[0]["start_time"] if results else None,
        "finished_at": datetime.now().isoformat(),
        "data_dir": str(args.data_dir),
        "h5ad_subdir": args.h5ad_subdir,
        "output_dir": str(args.output_dir),
        "total": len(results),
        "success": sum(1 for r in results if r.get("success")),
        "failed": sum(1 for r in results if not r.get("success")),
        "skipped": sum(1 for r in results if r.get("status") == "skipped"),
        "dry_run": args.dry_run,
        "results": results,
    }

    summary_path = args.output_dir / "BATCH_SUMMARY.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("\n" + "-" * 72)
    print(f"Batch complete. Summary written to: {summary_path}")
    print(
        f"Total={summary['total']} Success={summary['success']} Failed={summary['failed']} Skipped={summary['skipped']}"
    )
    print("-" * 72)


if __name__ == "__main__":
    main()


# Dry run:
# python run_all_tissues.py --dry-run
# Only selected tissues:
# python run_all_tissues.py --tissues brain breast colorectal
# Use clustered inputs:
# python run_all_tissues.py --h5ad-subdir h5ad_unlabelled_clustered
# Skip files already processed:
# python run_all_tissues.py --skip-existing
# Custom output directory:
# python run_all_tissues.py --output-dir /your/output/path
# Omit full logs in each JSON:
# python run_all_tissues.py --no-log