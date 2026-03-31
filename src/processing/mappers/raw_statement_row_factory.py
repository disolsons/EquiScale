from src.processing.mappers.model.raw_statement_row import RawStatementRow


class RawStatementRowFactory:
    @staticmethod
    def from_statement_dataframe(df, statement_type: str, ticker: str | None = None) -> list[RawStatementRow]:
        if df is None or df.empty:
            return []
 
        metadata_cols = {
            "label",
            "depth",
            "is_abstract",
            "is_total",
            "section",
            "confidence",
            "non_null_periods",
            "parent_raw_tag",
            "parent_label",
            "calculation_weight",
            "source_layer",
            "source_concept",
            "filing_accession",
            "filing_date",
        }

        period_cols = [c for c in df.columns if c not in metadata_cols]

        rows: list[RawStatementRow] = []

        for raw_tag, row in df.iterrows():
            values_by_period = {
                period: row.get(period)
                for period in period_cols
            }

            raw_row = RawStatementRow(
                raw_tag=str(raw_tag),
                label=row.get("label"),
                statement_type=statement_type,
                values_by_period=values_by_period,
                is_total=row.get("is_total"),
                is_abstract=row.get("is_abstract"),
                depth=row.get("depth"),
                section=row.get("section"),
                confidence=row.get("confidence"),
                non_null_periods=row.get("non_null_periods"),
                parent_raw_tag=row.get("parent_raw_tag"),
                parent_label=row.get("parent_label"),
                calculation_weight=row.get("calculation_weight"),
                source_layer=row.get("source_layer"),
                ticker=ticker,
                source_concept=row.get("source_concept"),
                filing_accession=row.get("filing_accession"),
                filing_date=row.get("filing_date"),
            ).with_computed_fields()

            rows.append(raw_row)

        return rows