from etl.jobs.load_dmi import run as run_dmi
from etl.jobs.load_spac import run as run_spac


def run() -> None:
    run_dmi()
    run_spac()


def main():
    run()
    print("All jobs completed successfully.")


if __name__ == "__main__":
    main()