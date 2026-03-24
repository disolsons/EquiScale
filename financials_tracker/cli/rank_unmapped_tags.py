import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Rank aggregated unmapped tags by mapping priority."
    )
    parser.add_argument(
        "--input-json",
        type=str,
        default="outputs/_analytics/aggregated/unmapped_tags.json",
        help="Path to aggregated unmapped tags JSON",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="outputs/_analytics/ranked",
        help="Directory to save ranked outputs",
    )
    return parser.parse_args()


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def has_false(values):
    return False in values


def has_true(values):
    return True in values


def min_numeric(values):
    numeric = [v for v in values if isinstance(v, (int, float))]
    return min(numeric) if numeric else None


def contains_additional(section_values):
    return any(
        isinstance(v, str) and "additional" in v.lower()
        for v in section_values
    )


def compute_priority_score(row: dict) -> int:
    score = 0

    ticker_count = row.get("ticker_count", 0) or 0
    max_non_null_periods = row.get("max_non_null_periods", 0) or 0
    is_abstract_values = row.get("is_abstract_values", []) or []
    is_total_values = row.get("is_total_values", []) or []
    depth_values = row.get("depth_values", []) or []
    avg_confidence = row.get("avg_confidence", None)
    section_values = row.get("section_values", []) or []

    score += ticker_count * 4
    score += max_non_null_periods * 2

    if has_false(is_abstract_values):
        score += 2
    elif has_true(is_abstract_values):
        score -= 10

    if has_true(is_total_values):
        score += 2

    min_depth = min_numeric(depth_values)
    if min_depth is not None and min_depth <= 1:
        score += 1

    if avg_confidence is not None and avg_confidence >= 0.5:
        score += 1

    if contains_additional(section_values):
        score -= 2

    return score


def assign_priority_bucket(row: dict) -> str:
    is_abstract_values = row.get("is_abstract_values", []) or []
    score = row["priority_score"]

    if has_true(is_abstract_values):
        return "ignore"

    if score >= 10:
        return "map_now"

    if score >= 5:
        return "later"

    return "ignore"


def rank_unmapped_tags(rows: list[dict]) -> list[dict]:
    ranked = []

    for row in rows:
        new_row = dict(row)
        new_row["priority_score"] = compute_priority_score(new_row)
        new_row["priority_bucket"] = assign_priority_bucket(new_row)
        ranked.append(new_row)

    ranked.sort(
        key=lambda x: (
            x["statement_type"],
            -x["priority_score"],
            -x["ticker_count"],
            x["raw_tag"],
        )
    )

    return ranked


def save_json(data, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def save_csv(data, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

    flattened_rows = []
    for row in data:
        flattened_rows.append({
            "statement_type": row["statement_type"],
            "raw_tag": row["raw_tag"],
            "priority_score": row["priority_score"],
            "priority_bucket": row["priority_bucket"],
            "ticker_count": row["ticker_count"],
            "count": row["count"],
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


def main():
    args = parse_args()

    input_json = Path(args.input_json)
    output_dir = Path(args.output_dir)

    rows = load_json(input_json)
    ranked = rank_unmapped_tags(rows)

    save_json(ranked, output_dir / "ranked_unmapped_tags.json")
    save_csv(ranked, output_dir / "ranked_unmapped_tags.csv")

    print(f"Ranked {len(ranked)} unmapped tag entries.")
    print(f"Saved to: {output_dir}")


if __name__ == "__main__":
    main()