"""Compatibility entry point for Prometheus observation export."""
import argparse
from benchlib.observations import capture_metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report')
    args = parser.parse_args()
    capture_metrics(args.report)


if __name__ == '__main__':
    main()
