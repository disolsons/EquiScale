
from src.clients.edgar.edgar_client import EdgarClient
from edgar import Company

def main():
    client = EdgarClient(concept_helper=None, ticker="NVDA")
    company = Company("NVDA")
    filings = company.get_filings(form="10-K")

    for filing in filings.head(4):
        print(filing.filing_date, filing.accession_number)
        print(filing)

    #hist_df = client.fetch_cash_flow(period_mode="history", years=3, annual=True)

    #print(hist_df.index.tolist())

    # if "label" in hist_df.columns:
    #     print(hist_df[hist_df["label"].astype(str).str.contains(
    #         "property|equipment|productive|capital|acquire",
    #         case=False,
    #         na=False
    #     )].to_string())
    facts = company.get_facts()
    print("TYPE:", type(facts))
    print("METHODS:")
    for name in dir(facts):
        if not name.startswith("_"):
            print(name)

    for name in ["query", "get_fact", "get_facts", "to_dataframe", "pivot_by_period"]:
        print(name, hasattr(facts, name))
    # latest_df = client.fetch_cash_flow(period_mode="latest", annual=True)

    # print(latest_df[latest_df.astype(str).apply(
    #     lambda col: col.str.contains("PaymentsToAcquireProductiveAssets", na=False)
    # ).any(axis=1)])

    # print(client.search_capex_rows(period_mode="history", years=3, annual=True))
    # print(client.search_capex_rows(period_mode="latest", annual=True))

    # results = client.inspect_capex_facts("NVDA")
    # for concept_name, payload in results.items():
    #     print("=" * 80)
    #     print(concept_name)
    #     print(payload)

if __name__ == "__main__":
    main()