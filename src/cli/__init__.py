import argparse
import sys
from cli import ingest, transform


def main():
    parser = argparse.ArgumentParser(prog="btc")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Upload a raw BTC partition to S3")
    ingest_parser.add_argument("date", help="Partition date (YYYY-MM-DD)")

    transform_parser = subparsers.add_parser("transform", help="Compute gold metrics from raw partition")
    transform_parser.add_argument("date", help="Partition date (YYYY-MM-DD)")

    args = parser.parse_args()
    try:
        if args.command == "ingest":
            ingest.run(args)
        elif args.command == "transform":
            transform.run(args)
    except (ValueError, EnvironmentError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
