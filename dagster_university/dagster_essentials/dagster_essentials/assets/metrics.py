import os

import dagster as dg
import duckdb
import geopandas as gpd
import matplotlib.pyplot as plt
from dagster._utils.backoff import backoff
from dagster_essentials.assets import constants


@dg.asset(deps=["taxi_trips", "taxi_zones"])
def manhattan_stats() -> dg.MaterializeResult:
    """
    Stats
    """
    query = """
    select
    zones.zone,
    zones.borough,
    zones.geometry,
    count(1) as num_trips,
    from trips
    left join zones on trips.pickup_zone_id = zones.zone_id
    where borough = 'Manhattan' and geometry is not null
    group by zone, borough, geometry
    """

    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))  # pyright: ignore [reportArgumentType]
    trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])

    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)
    preview_df = trips_by_zone[:5].to_markdown(index=False)

    with open(constants.MANHATTAN_STATS_FILE_PATH, "w") as output_file:
        output_file.write(trips_by_zone.to_json())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(trips_by_zone.shape[0]),
            "preview": dg.MetadataValue.md(preview_df),  # pyright: ignore [reportArgumentType]
        }
    )


@dg.asset(deps=["manhattan_stats"])
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(
        column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black"
    )
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # pyright: ignore [reportArgumentType]
    ax.set_ylim([40.70, 40.82])  # pyright: ignore [reportArgumentType]

    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)


@dg.asset(deps=["taxi_trips"])
def trips_by_week() -> dg.MaterializeResult:
    """
    Produces a CSV.
    """
    query = """
    select
    date_trunc('week', pickup_datetime) + interval '1 week' as period,
    count(1) as num_trips,
    sum(passenger_count) as passenger_count,
    sum(total_amount) as total_amount,
    sum(trip_distance) as trip_distance
    from trips
    group by date_trunc('week', pickup_datetime)
    """

    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )
    trips = conn.execute(query).fetch_df()
    preview_df = trips[:5].to_markdown(index=False)

    with open(constants.TRIPS_BY_WEEK_FILE_PATH, "w") as output_file:
        output_file.write(trips.to_csv())

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(trips.shape[0]),
            "preview": dg.MetadataValue.md(preview_df),  # pyright: ignore [reportArgumentType]
        }
    )
