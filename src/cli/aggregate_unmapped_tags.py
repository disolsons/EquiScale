import argparse
import json
from pathlib import Path
from src.storage.db_setup import get_session_factory
from src.storage.repositories import replace_aggregated_unmapped_tags

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aggregate unmapped tags across ticker validation reports."
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="outputs",
        help="Base directory containing per-ticker output folders",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="outputs/_analytics/aggregated",
        help="Directory where aggregated outputs will be written",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def normalize_list_value(value):
    if value is None:
        return None
    if isinstance(value, list):
        return tuple(value)
    return value


def aggregate_unmapped_tags(input_dir: Path) -> list[dict]:
    """
    Scan validation reports under:
      outputs/<TICKER>/validation/<statement_type>.json

    Aggregate unmapped tags across tickers and statement types.
    """
    aggregated = {}

    validation_files = list(input_dir.glob("*/validation/*.json"))

    for validation_file in validation_files:
        ticker = validation_file.parent.parent.name.upper()
        statement_type = validation_file.stem

        report = load_json(validation_file)
        unmapped_section = report.get("unmapped_tags", {})
        unmapped_tags = unmapped_section.get("unmapped_raw_tags", [])

        for entry in unmapped_tags:
            raw_tag = entry.get("raw_tag")
            if not raw_tag:
                continue

            key = (statement_type, raw_tag)

            if key not in aggregated:
                aggregated[key] = {
                    "statement_type": statement_type,
                    "raw_tag": raw_tag,
                    "count": 0,
                    "tickers": set(),
                    "example_labels": set(),
                    "non_null_period_counts": [],
                    "is_abstract_values": set(),
                    "is_total_values": set(),
                    "depth_values": set(),
                    "section_values": set(),
                    "confidence_values": [],
                }

            row = aggregated[key]
            row["count"] += 1
            row["tickers"].add(ticker)

            label = entry.get("label")
            if label:
                row["example_labels"].add(label)

            non_null_periods = entry.get("non_null_periods")
            if non_null_periods is not None:
                row["non_null_period_counts"].append(non_null_periods)

            row["is_abstract_values"].add(normalize_list_value(entry.get("is_abstract")))
            row["is_total_values"].add(normalize_list_value(entry.get("is_total")))
            row["depth_values"].add(normalize_list_value(entry.get("depth")))
            row["section_values"].add(normalize_list_value(entry.get("section")))

            confidence = entry.get("confidence")
            if confidence is not None:
                row["confidence_values"].append(confidence)

    results = []
    for (_, _), row in aggregated.items():
        results.append({
            "statement_type": row["statement_type"],
            "raw_tag": row["raw_tag"],
            "count": row["count"],
            "ticker_count": len(row["tickers"]),
            "tickers": sorted(row["tickers"]),
            "example_labels": sorted(row["example_labels"]),
            "max_non_null_periods": max(row["non_null_period_counts"]) if row["non_null_period_counts"] else None,
            "avg_non_null_periods": (
                sum(row["non_null_period_counts"]) / len(row["non_null_period_counts"])
                if row["non_null_period_counts"] else None
            ),
            "is_abstract_values": sorted(v for v in row["is_abstract_values"] if v is not None),
            "is_total_values": sorted(v for v in row["is_total_values"] if v is not None),
            "depth_values": sorted(v for v in row["depth_values"] if v is not None),
            "section_values": sorted(v for v in row["section_values"] if v is not None),
            "avg_confidence": (
                sum(row["confidence_values"]) / len(row["confidence_values"])
                if row["confidence_values"] else None
            ),
        })

    results.sort(
        key=lambda x: (
            x["statement_type"],
            -x["ticker_count"],
            -x["count"],
            x["raw_tag"],
        )
    )

    return results


def save_json(data: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def save_csv(data: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    flattened_rows = []
    for row in data:
        flattened_rows.append({
            "statement_type": row["statement_type"],
            "raw_tag": row["raw_tag"],
            "count": row["count"],
            "ticker_count": row["ticker_count"],
            "tickers": ", ".join(row["tickers"]),
            "example_labels": " | ".join(row["example_labels"]),
            "max_non_null_periods": row["max_non_null_periods"],
            "avg_non_null_periods": row["avg_non_null_periods"],
            "is_abstract_values": ", ".join(map(str, row["is_abstract_values"])),
            "is_total_values": ", ".join(map(str, row["is_total_values"])),
            "depth_values": ", ".join(map(str, row["depth_values"])),
            "section_values": ", ".join(map(str, row["section_values"])),
            "avg_confidence": row["avg_confidence"],
        })

    df = pd.DataFrame(flattened_rows)
    df.to_csv(path, index=False)

def persist_aggregated_results_to_db(aggregated: list[dict]):
    SessionFactory = get_session_factory()
    session = SessionFactory()

    try:
        replace_aggregated_unmapped_tags(session, aggregated)
        session.commit()
    finally:
        session.close()

def main():
    args = parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    aggregated = aggregate_unmapped_tags(input_dir)

    save_json(aggregated, output_dir / "unmapped_tags.json")
    save_csv(aggregated, output_dir / "unmapped_tags.csv")
    persist_aggregated_results_to_db(aggregated)
    
    print(f"Aggregated {len(aggregated)} unique unmapped tag entries.")
    print(f"Saved to: {output_dir}")


if __name__ == "__main__":
    main()