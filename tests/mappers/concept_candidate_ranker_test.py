import pytest

from financials_tracker.mappers.model.concept_candidate import ConceptCandidate
from financials_tracker.mappers.concept_candidate_ranker import ConceptCandidateRanker


@pytest.fixture
def ranker() -> ConceptCandidateRanker:
    return ConceptCandidateRanker()


def make_candidate(
    raw_tag: str,
    *,
    is_abstract: bool | None = False,
    is_total: bool | None = False,
    depth: int | None = 1,
    non_null_periods: int = 3,
    label: str | None = None,
) -> ConceptCandidate:
    return ConceptCandidate(
        raw_tag=raw_tag,
        label=label or raw_tag,
        is_abstract=is_abstract,
        is_total=is_total,
        depth=depth,
        non_null_periods=non_null_periods,
        row_data={"FY 2025": 1, "FY 2024": 1, "FY 2023": 1},
    )


def test_select_best_candidate_rejects_abstract_rows(ranker: ConceptCandidateRanker):
    abstract_candidate = make_candidate(
        "AssetsAbstract",
        is_abstract=True,
        is_total=False,
        depth=0,
        non_null_periods=0,
    )
    valid_candidate = make_candidate(
        "Assets",
        is_abstract=False,
        is_total=True,
        depth=1,
        non_null_periods=3,
    )

    result = ranker.select_best_candidate("assets", [abstract_candidate, valid_candidate])

    assert result is not None
    assert result.raw_tag == "Assets"


def test_select_best_candidate_prefers_more_non_null_periods(ranker: ConceptCandidateRanker):
    weaker = make_candidate(
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        is_total=True,
        depth=1,
        non_null_periods=1,
    )
    stronger = make_candidate(
        "SalesRevenueNet",
        is_total=True,
        depth=1,
        non_null_periods=3,
    )

    result = ranker.select_best_candidate("sales_revenue_net", [weaker, stronger])

    assert result is not None
    assert result.raw_tag == "SalesRevenueNet"


def test_select_best_candidate_prefers_total_when_other_signals_are_similar(ranker: ConceptCandidateRanker):
    non_total = make_candidate(
        "LongTermDebtNoncurrent",
        is_total=False,
        depth=1,
        non_null_periods=3,
    )
    total_like = make_candidate(
        "LongTermDebt",
        is_total=True,
        depth=1,
        non_null_periods=3,
    )

    result = ranker.select_best_candidate("long_term_debt", [non_total, total_like])

    assert result is not None
    assert result.raw_tag == "LongTermDebt"


def test_select_best_candidate_prefers_shallower_depth_when_other_signals_are_equal(
    ranker: ConceptCandidateRanker,
):
    shallow = make_candidate(
        "OperatingIncomeLoss",
        is_total=True,
        depth=1,
        non_null_periods=3,
    )
    deep = make_candidate(
        "OperatingIncomeLossDetailed",
        is_total=True,
        depth=3,
        non_null_periods=3,
    )

    result = ranker.select_best_candidate("operating_income_loss", [deep, shallow])

    assert result is not None
    assert result.raw_tag == "OperatingIncomeLoss"


def test_select_best_candidate_returns_none_when_all_candidates_are_abstract(
    ranker: ConceptCandidateRanker,
):
    candidate_a = make_candidate(
        "AssetsAbstract",
        is_abstract=True,
        depth=0,
        non_null_periods=0,
    )
    candidate_b = make_candidate(
        "LiabilitiesAbstract",
        is_abstract=True,
        depth=0,
        non_null_periods=0,
    )

    result = ranker.select_best_candidate("assets", [candidate_a, candidate_b])

    assert result is None