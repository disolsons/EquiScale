from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
)

from financials_tracker.storage.db_setup import Base


class MappingValidations(Base):
    __tablename__ = "mapping_validations"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    statement_type = Column(String, nullable=False, index=True)

    expected_concepts_count = Column(Integer, nullable=False)
    mapped_concepts_count = Column(Integer, nullable=False)
    coverage_ratio = Column(Float, nullable=False)

    reconciliation_passed = Column(Boolean, nullable=True)
    reconciliation_method = Column(String, nullable=True)

    missing_concepts_count = Column(Integer, nullable=False)
    unmapped_tag_count = Column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint("ticker", "statement_type", name="uq_mapping_validation_ticker_statement"),
    )

class UnmappedTags(Base):
    __tablename__ = "unmapped_tags"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    statement_type = Column(String, nullable=False, index=True)

    raw_tag = Column(String, nullable=False, index=True)
    label = Column(Text, nullable=True)

    non_null_periods = Column(Integer, nullable=True)
    is_abstract = Column(Boolean, nullable=True)
    is_total = Column(Boolean, nullable=True)
    depth = Column(Integer, nullable=True)
    section = Column(String, nullable=True)
    confidence = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "ticker",
            "statement_type",
            "raw_tag",
            name="uq_unmapped_tag_ticker_statement_rawtag",
        ),
    )

class AggregatedUnmappedTags(Base):
    __tablename__ = "aggregated_unmapped_tags"

    id = Column(Integer, primary_key=True)
    statement_type = Column(String, nullable=False, index=True)
    raw_tag = Column(String, nullable=False, index=True)

    count = Column(Integer, nullable=False)
    ticker_count = Column(Integer, nullable=False)

    tickers = Column(Text, nullable=True)
    example_labels = Column(Text, nullable=True)

    max_non_null_periods = Column(Integer, nullable=True)
    avg_non_null_periods = Column(Float, nullable=True)

    is_abstract_values = Column(Text, nullable=True)
    is_total_values = Column(Text, nullable=True)
    depth_values = Column(Text, nullable=True)
    section_values = Column(Text, nullable=True)

    avg_confidence = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "statement_type",
            "raw_tag",
            name="uq_aggregated_unmapped_statement_rawtag",
        ),
    )

class TagSuggestions(Base):
    __tablename__ = "tag_suggestions"

    id = Column(Integer, primary_key=True)
    statement_type = Column(String, nullable=False, index=True)
    raw_tag = Column(String, nullable=False, index=True)

    suggested_concept = Column(String, nullable=True)
    suggestion_type = Column(String, nullable=False)
    suggestion_confidence = Column(Float, nullable=False)
    suggestion_reason = Column(Text, nullable=True)
    ticker_count = Column(Integer, nullable=True)
    priority_score = Column(Float, nullable=True)
    priority_bucket = Column(String, nullable=True)
    source = Column(String, nullable=False, default="concept_inference_engine")

    __table_args__ = (
        UniqueConstraint(
            "statement_type",
            "raw_tag",
            name="uq_tag_suggestion_statement_rawtag",
        ),
    )

class MappedConceptSelections(Base):
    __tablename__ = "mapped_concept_selections"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    statement_type = Column(String, nullable=False, index=True)
    concept = Column(String, nullable=False, index=True)

    raw_tag = Column(String, nullable=False)
    label = Column(Text, nullable=True)

    is_abstract = Column(Boolean, nullable=True)
    is_total = Column(Boolean, nullable=True)
    depth = Column(Integer, nullable=True)
    non_null_periods = Column(Integer, nullable=True)

    candidate_score = Column(Float, nullable=True)
    is_selected = Column(Boolean, nullable=False, default=False)
    rank_order = Column(Integer, nullable=True)
    candidate_count = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "ticker",
            "statement_type",
            "concept",
            "raw_tag",
            name="uq_mapped_concept_selection_candidate",
        ),
    )

class MappedConceptValues(Base):
    __tablename__ = "mapped_concept_values"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    statement_type = Column(String, nullable=False, index=True)
    concept = Column(String, nullable=False, index=True)
    period = Column(String, nullable=False, index=True)
    value = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "ticker",
            "statement_type",
            "concept",
            "period",
            name="uq_mapped_concept_value",
        ),
    )