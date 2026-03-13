from etl.jobs.load_dmi import run as run_dmi
from etl.jobs.load_spac import run as run_spac


def run() -> None:
    run_dmi()
    run_spac()