# Weather ETL with Docker Compose

This project loads weather and sensor data from external APIs into a local PostgreSQL database.

The main workflow is built around Docker Compose:
- PostgreSQL runs in a container
- the Python app connects to it
- ETL jobs load and update data incrementally

A small local HTTP plot server also exists, but it is still a work in progress.

## Requirements

- Python 3.10
- Docker Desktop
- A `.env` file in the project root

## Environment variables

Create a `.env` file with:

    POSTGRES_USER=your_user
    POSTGRES_PASSWORD=your_password
    POSTGRES_DB=your_database
    DB_HOST=db
    DB_PORT=5432
    SPAC_API_TOKEN=your_token

If you want to run Python locally instead of inside Docker, use:

    DB_HOST=localhost
    DB_PORT=5433
    RUNNING_LOCALLY=True

## Docker workflow

Start the database:

    docker compose up -d db

This starts PostgreSQL in a container and exposes it on local port `5433`.

Build and run the app container:

    docker compose up --build app

Because the app depends on the database health check, it waits until PostgreSQL is ready before starting.

Stop everything:

    docker compose down

Stop everything and remove the database volume:

    docker compose down -v

## Running ETL jobs locally

If you want to run the Python app outside Docker:

1. create a virtual environment
2. install dependencies
3. set `RUNNING_LOCALLY=True` in environment variables
4. make sure PostgreSQL is running with Docker Compose

Install dependencies:

### Windows

    python -m venv .venv
    .venv\Scripts\activate
    pip install -r requirements.txt

### macOS/Linux

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

Run ETL jobs:

    python main.py dmi
    python main.py spac
    python main.py all

## What the ETL does

The ETL pipeline:
- fetches weather data from DMI
- fetches sensor data from the SPAC API
- creates database tables if needed
- inserts only new data based on timestamps and conflict handling

## Database

Data is stored in PostgreSQL tables for:
- DMI weather observations
- BME280 sensor readings
- DS18B20 sensor readings

The PostgreSQL data directory is mounted as a Docker volume so data persists between container restarts.

## HTTP server

There is also a simple local HTTP server for plotting database data with Matplotlib.

Run it locally with:

    python -m etl.server.server

Then open:

    http://127.0.0.1:8000

This part is experimental and still under development.

## Common commands

Start database only:

    docker compose up -d db

Run full app stack:

    docker compose up --build

View logs:

    docker compose logs -f

Stop containers:

    docker compose down

Reset database:

    docker compose down -v

## Notes

- Use `db:5432` when the Python app runs inside Docker
- Use `localhost:5433` when the Python app runs locally
- The database container uses a named Docker volume for persistence
