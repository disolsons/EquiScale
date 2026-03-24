import argparse
from pathlib import Path

from financials_tracker.mappers.concept_inference.concept_aliases_helper import ConceptAliasesHelper
from financials_tracker.mappers.concept_inference.concept_inference_engine import ConceptInferenceEngine
from financials_tracker.mappers.concept_inference.fuzzy_concept_matcher import FuzzyConceptMatcher
from financials_tracker.mappers.concept_inference.ignore_patterns_helper import IgnorePatternsHelper
from financials_tracker.mappers.concept_inference.semantic_conflicts_helper import SemanticConflictsHelper
from financials_tracker.storage.db_setup import get_session_factory
from financials_tracker.storage.models import AggregatedUnmappedTag
from financials_tracker.storage.repositories import (
    delete_all_tag_suggestions,
    upsert_tag_suggestion,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONCEPT_ALIASES_PATH = (
    PROJECT_ROOT
    / "financials_tracker"
    / "mappers"
    / "config"
    / "concept_aliases.yaml"
)

IGNORE_PATTERNS_PATH = (
    PROJECT_ROOT
    / "financials_tracker"
    / "mappers"
    / "config"
    / "ignore_patterns.yaml"
)

SEMANTIC_CONFLICTS_PATH = (
    PROJECT_ROOT
    / "financials_tracker"
    / "mappers"
    / "config"
    / "semantic_conflicts.yaml"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate concept suggestions for aggregated unmapped tags."
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Optional SQLite database path override.",
    )
    parser.add_argument(
        "--statement-type",
        type=str,
        default=None,
        help="Optional statement type filter (income_statement, balance_sheet, cash_flow).",
    )
    parser.add_argument(
        "--min-ticker-count",
        type=int,
        default=1,
        help="Only process aggregated tags seen in at least this many tickers.",
    )
    parser.add_argument(
        "--exclude-ignore-bucket",
        action="store_true",
        help="Skip rows whose priority_bucket is 'ignore' (if that column exists in your DB).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of rows to process.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run inference without writing suggestions to the database.",
    )
    return parser.parse_args()


def build_inference_engine() -> ConceptInferenceEngine:
    aliases_helper = ConceptAliasesHelper(CONCEPT_ALIASES_PATH)
    ignore_patterns_helper = IgnorePatternsHelper(IGNORE_PATTERNS_PATH)
    semantic_conflicts_helper = SemanticConflictsHelper(SEMANTIC_CONFLICTS_PATH)
    fuzzy_matcher = FuzzyConceptMatcher(semantic_conflicts_helper)

    return ConceptInferenceEngine(
        concept_aliases_helper=aliases_helper,
        ignore_patterns_helper=ignore_patterns_helper,
        semantic_conflicts_helper=semantic_conflicts_helper,
        fuzzy_matcher=fuzzy_matcher,
    )


def build_query(session, args: argparse.Namespace):
    query = session.query(AggregatedUnmappedTag)

    if args.statement_type:
        query = query.filter(
            AggregatedUnmappedTag.statement_type == args.statement_type
        )

    query = query.filter(
        AggregatedUnmappedTag.ticker_count >= args.min_ticker_count
    )

    # Optional filter only if your model has this column
    if args.exclude_ignore_bucket and hasattr(AggregatedUnmappedTag, "priority_bucket"):
        query = query.filter(AggregatedUnmappedTag.priority_bucket != "ignore")

    query = query.order_by(
        AggregatedUnmappedTag.statement_type.asc(),
        AggregatedUnmappedTag.ticker_count.desc(),
        AggregatedUnmappedTag.raw_tag.asc(),
    )

    if args.limit:
        query = query.limit(args.limit)

    return query


def main() -> None:
    args = parse_args()

    SessionFactory = get_session_factory(args.db_path)
    session = SessionFactory()
    engine = build_inference_engine()

    try:

        if not args.dry_run:
            delete_all_tag_suggestions(session)
            session.commit()
            print("Cleared existing tag suggestions.")
            
        rows = build_query(session, args).all()
        print(f"Found {len(rows)} aggregated unmapped tags to process.")

        processed = 0
        written = 0

        for row in rows:
            suggestion = engine.suggest_concept(
                statement_type=row.statement_type,
                raw_tag=row.raw_tag,
                label=extract_label(row),
            )

            print(
                f"[{row.statement_type}] {row.raw_tag} -> "
                f"{suggestion['suggested_concept']} "
                f"({suggestion['suggestion_type']}, {suggestion['suggestion_confidence']})"
            )

            if not args.dry_run:
                upsert_tag_suggestion(
                    session=session,
                    statement_type=row.statement_type,
                    raw_tag=row.raw_tag,
                    suggestion=suggestion,
                )
                written += 1

            processed += 1

        if not args.dry_run:
            session.commit()

        print(f"Processed {processed} rows.")
        if not args.dry_run:
            print(f"Wrote {written} suggestions to the database.")

    finally:
        session.close()

def extract_label(row) -> str | None:
    raw = getattr(row, "example_labels", None)
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list) and parsed:
            return parsed[0]
    except Exception:
        return raw
    return None

if __name__ == "__main__":
    main()