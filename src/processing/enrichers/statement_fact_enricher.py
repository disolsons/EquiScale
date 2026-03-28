import pandas as pd


class StatementFactEnricher:
    """
    Enrich a historical statement DataFrame with missing direct-mapping tags
    queried from EntityFacts.

    This sits ABOVE EdgarClient:
    - EdgarClient fetches raw statement frames and raw concept-query DataFrames
    - StatementFactEnricher uses the concept map to decide what to recover
    """

    def __init__(self, concept_helper, edgar_client):
        self.concept_helper = concept_helper
        self.edgar_client = edgar_client

    def enrich_missing_direct_tags(
        self,
        statement_df: pd.DataFrame | None,
        statement_type: str,
        ticker: str,
        years: int = 5,
        annual: bool = True,
    ) -> pd.DataFrame | None:
        """
        Backfill direct-mapping tags that are defined in concept_map but missing
        from the statement-oriented historical DataFrame.
        """
        if statement_df is None or statement_df.empty:
            return statement_df

        base_df = statement_df.copy()
        base_df = self._ensure_metadata_columns(base_df, source_layer="statement_frame")

        expected_tags = self._get_all_direct_mapping_tags_for_statement(statement_type)
        existing_tags = {str(idx) for idx in base_df.index}

        missing_tags = [tag for tag in expected_tags if tag not in existing_tags]
        if not missing_tags:
            return base_df

        extra_rows: list[dict] = []
        target_period_cols = [
            col for col in base_df.columns
            if str(col).startswith("FY ") or str(col).startswith(("Q1 ", "Q2 ", "Q3 ", "Q4 "))
        ]

        for raw_tag in missing_tags:
            print(f"[DEBUG] trying backfill tag: {raw_tag}")


            query_df = self.edgar_client.query_concept_facts(
                raw_tag=raw_tag,
                ticker=ticker,
                years=years,
                annual=annual,
            )

            if query_df is None:
                print(f"[DEBUG] query returned None for {raw_tag}")
            else:
                print(f"[DEBUG] query returned {len(query_df)} rows for {raw_tag}")
                print(query_df.to_string())

            if query_df is None or query_df.empty:
                continue
 
            row = self._convert_query_df_to_statement_row(
                raw_tag=raw_tag,
                query_df=query_df,
                annual=annual,
                allowed_period_cols=target_period_cols,
            )

            if row is not None:
                extra_rows.append(row)

        if not extra_rows:
            return base_df

        extra_df = pd.DataFrame(extra_rows).set_index("concept")
        extra_df = self._ensure_metadata_columns(extra_df, source_layer="facts_query_backfill")

        # Align columns before concat
        all_cols = list(dict.fromkeys(list(base_df.columns) + list(extra_df.columns)))
        base_df = base_df.reindex(columns=all_cols)
        extra_df = extra_df.reindex(columns=all_cols)

        combined = pd.concat([base_df, extra_df], axis=0)

        # Keep statement-frame rows first if any duplicate index appears
        combined = combined[~combined.index.duplicated(keep="first")]

        return combined

    def _get_all_direct_mapping_tags_for_statement(self, statement_type: str) -> list[str]:
        """
        Flatten all direct-mapping raw tags for one statement.

        Supports:
        - current simple list schema:
            concept: [tag1, tag2]
        - future richer schema:
            concept:
              direct_mapping_tags: [...]
        """
        statement_map = self.concept_helper.concept_map.get(statement_type, {}) or {}
        tags: list[str] = []

        for _, concept_value in statement_map.items():
            if isinstance(concept_value, list):
                tags.extend(concept_value)
            elif isinstance(concept_value, dict):
                direct_tags = concept_value.get("direct_mapping_tags", []) or []
                tags.extend(direct_tags)

        return list(dict.fromkeys(tags))

    def _convert_query_df_to_statement_row(
        self,
        raw_tag: str,
        query_df: pd.DataFrame,
        annual: bool = True,
        allowed_period_cols: list[str] | None = None,
    ) -> dict | None:
        """
        Convert a concept-specific query DataFrame into one statement-style row.

        This version matches the actual query output shape observed in your environment:
        - numeric_value
        - fiscal_year
        - fiscal_period
        - period_end
        - label
        """

        df = query_df.copy()

        # Filter annual / quarterly using actual column names from query output
        if "fiscal_period" in df.columns:
            if annual:
                df = df[df["fiscal_period"] == "FY"]
            else:
                df = df[df["fiscal_period"].isin(["Q1", "Q2", "Q3", "Q4"])]

        if df.empty:
            return None

        # Sort latest first if available
        if "period_end" in df.columns:
            df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce")
            df = df.sort_values("period_end", ascending=False)
            df = df.drop_duplicates(subset=["period_end"], keep="first")

        period_values = {}

        for _, row in df.iterrows():
            fy = row.get("fiscal_year")
            fp = row.get("fiscal_period")
            val = row.get("numeric_value")
            period_end = row.get("period_end")

            if val is None:
                continue

            if annual:
                if period_end is None:
                    continue
                col_year = pd.to_datetime(period_end).year
                col = f"FY {col_year}"
            else:
                if fy is None or fp is None:
                    continue
                col = f"{fp} {int(fy)}"

            period_values[col] = val

        if allowed_period_cols is not None:  
            period_values = {
                k: v for k, v in period_values.items()
                if k in allowed_period_cols
            }

        if not period_values:
            return None

        first = df.iloc[0]
        return {
            "concept": raw_tag,
            "label": first.get("label", raw_tag),
            "depth": None,
            "is_abstract": False,
            "is_total": False,
            "section": None,
            "confidence": None,
            "parent_raw_tag": None,
            "parent_label": None,
            "calculation_weight": None,
            "source_layer": "facts_query_backfill",
            "source_concept": first.get("concept"),
            "filing_accession": first.get("accession"),
            "filing_date": first.get("filing_date"),
            **period_values,
        }

    @staticmethod
    def _normalize_parent_tag(parent_raw_tag):
        if parent_raw_tag is None:
            return None

        parent_raw_tag = str(parent_raw_tag)

        if parent_raw_tag.startswith("us-gaap:"):
            return parent_raw_tag.split(":", 1)[1]

        if parent_raw_tag.startswith("us-gaap_"):
            return parent_raw_tag.split("_", 1)[1]

        return parent_raw_tag

    @staticmethod
    def _ensure_metadata_columns(df: pd.DataFrame, source_layer: str) -> pd.DataFrame:
        out = df.copy()

        if "parent_raw_tag" not in out.columns:
            out["parent_raw_tag"] = None

        if "calculation_weight" not in out.columns:
            out["calculation_weight"] = None

        if "source_layer" not in out.columns:
            out["source_layer"] = source_layer

        return out