
from financials_tracker.clients.edgar.edgar_client import EdgarClient

def main():
    client = EdgarClient(concept_helper=None, ticker="NVDA")

    print(client.search_capex_rows(period_mode="history", years=3, annual=True))
    print(client.search_capex_rows(period_mode="latest", annual=True))

    results = client.inspect_capex_facts("NVDA")
    for concept_name, payload in results.items():
        print("=" * 80)
        print(concept_name)
        print(payload)
        
if __name__ == "__main__":
    main()