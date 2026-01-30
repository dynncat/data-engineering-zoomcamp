import pandas as pd
from sqlalchemy import create_engine
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"


@click.command()
@click.option("--year", default=2021, type=int, show_default=True, help="Dataset year (e.g., 2021)")
@click.option("--month", default=1, type=int, show_default=True, help="Dataset month (1-12)")
@click.option("--chunksize", default=100000, type=int, show_default=True, help="Read/insert chunk size")

@click.option("--pg-user", default="root", show_default=True, help="PostgreSQL user")
@click.option("--pg-pass", default="root", show_default=True, help="PostgreSQL password")
@click.option("--pg-host", default="localhost", show_default=True, help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, show_default=True, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", show_default=True, help="PostgreSQL database name")

@click.option("--target-table", default="yellow_taxi_data", show_default=True, help="Target table name")

def run(year, month, chunksize, pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
    if not (1 <= month <= 12):
        raise click.BadParameter("month must be between 1 and 12")

    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    click.echo(f"[INFO] url={url}")
    click.echo(f"[INFO] target_table={target_table}")
    click.echo(f"[INFO] chunksize={chunksize}")

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True
    total_rows = 0

    for df_chunk in df_iter:
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first = False
            click.echo("[INFO] table created (schema)")

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        total_rows += len(df_chunk)
        click.echo(f"[INFO] inserted: {len(df_chunk)} (total={total_rows})")

    click.echo("[SUCCESS] ingestion completed")


if __name__ == "__main__":
    run()
