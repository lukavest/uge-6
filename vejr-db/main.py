import argparse
import logging

from etl.jobs.load_all import run as run_all
from etl.jobs.load_dmi import run as run_dmi
from etl.jobs.load_spac import run as run_spac
from etl.utils.logging import configure_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ETL jobs")
    parser.add_argument("job", choices=["dmi", "spac", "all"])
    args = parser.parse_args()

    configure_logging(logging.INFO)

    if args.job == "dmi":
        run_dmi()
    elif args.job == "spac":
        run_spac()
    else:
        run_all()


if __name__ == "__main__":
    main()