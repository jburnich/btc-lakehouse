import argparse
import sys
from ingest import fetch_partition


def main():
    parser = argparse.ArgumentParser(description="Download a BTC transaction partition")
    parser.add_argument("date", help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()
    try:
        path = fetch_partition(args.date)
        print(f"Partition uploaded to {path}")
    except (ValueError, EnvironmentError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
