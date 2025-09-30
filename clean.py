import duckdb
import logging
from pathlib import Path
from datetime import datetime


DB_PATH = "nytaxi.db"  
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / "clean.log"


#setting u4 the logger for the clean file
logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s [CLEAN] %(levelname)s: %(message)s"
)
logger = logging.getLogger("clean")


#this logs a message and also prints it to terminal
def _log_and_print(msg):
    print(msg)
    logger.info(msg)



# this combines the yellow and green tables, then does applies some of cleaning rules, puts it in
# a new table and then run basic tests inside instead of in separate scripts. I had to have some help from chat for the last part
def clean_trips():
    con = None
    try:
        con = duckdb.connect(database=DB_PATH, read_only=False)

        
        con.execute("""
            DROP TABLE IF EXISTS trips_raw_2024;
            CREATE TABLE trips_raw_2024 AS
            SELECT
                'yellow' AS service,
                tpep_pickup_datetime   AS pickup_datetime,
                tpep_dropoff_datetime  AS dropoff_datetime,
                passenger_count,
                trip_distance
            FROM yellow_taxi
            UNION ALL
            SELECT
                'green' AS service,
                lpep_pickup_datetime   AS pickup_datetime,
                lpep_dropoff_datetime  AS dropoff_datetime,
                passenger_count,
                trip_distance
            FROM green_taxi;
        """)
        _log_and_print("Built unified table trips_raw_2024 from yellow_taxi + green_taxi.")

        

        con.execute("""
            DROP TABLE IF EXISTS trips_clean_2024;
            CREATE TABLE trips_clean_2024 AS
            WITH base AS (
                SELECT DISTINCT
                    service,
                    pickup_datetime,
                    dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    /* duration in hours (float) */
                    CAST(date_diff('minute', pickup_datetime, dropoff_datetime) AS DOUBLE) / 60.0 AS duration_hours
                FROM trips_raw_2024
            )
            SELECT
                service, pickup_datetime, dropoff_datetime,
                passenger_count, trip_distance
            FROM base
            WHERE
                passenger_count IS NOT NULL AND passenger_count > 0
                AND trip_distance IS NOT NULL AND trip_distance > 0 AND trip_distance <= 100
                AND dropoff_datetime IS NOT NULL AND pickup_datetime IS NOT NULL
                AND dropoff_datetime > pickup_datetime
                AND duration_hours > 0 AND duration_hours <= 24.0
            ;
        """)
        _log_and_print("Created trips_clean_2024 with filters and deduping applied.")

        con.execute("""
            DROP TABLE IF EXISTS trips_2024;
            ALTER TABLE trips_clean_2024 RENAME TO trips_2024;
        """)
        _log_and_print("Renamed trips_clean_2024 -> trips_2024.")



        tests = { #had some help from chat here
            "duplicates_removed": """
        WITH k AS (
            SELECT
                service, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
                COUNT(*) AS c
            FROM trips_2024
            GROUP BY 1,2,3,4,5
        )
        SELECT SUM(CASE WHEN c > 1 THEN 1 ELSE 0 END) AS dup_keys FROM k;
        """,
            "no_zero_passengers": """
                SELECT COUNT(*) FROM trips_2024
                WHERE passenger_count IS NULL OR passenger_count = 0;
            """,
            "no_zero_miles": """
                SELECT COUNT(*) FROM trips_2024
                WHERE trip_distance IS NULL OR trip_distance <= 0;
            """,
            "no_over_100_miles": """
                SELECT COUNT(*) FROM trips_2024
                WHERE trip_distance > 100;
            """,
            "no_over_24_hours": """
                SELECT COUNT(*) FROM (
                    SELECT CAST(date_diff('minute', pickup_datetime, dropoff_datetime) AS DOUBLE) / 60.0 AS duration_hours
                    FROM trips_2024
                )
                WHERE duration_hours > 24.0 OR duration_hours <= 0.0;
            """
        }

        #this is for checking if all the tests that i put above passed
        all_passed = True
        for name, sql in tests.items():
            cnt = con.execute(sql).fetchone()[0]
            msg = f"TEST {name}: offending rows = {int(cnt)}"
            _log_and_print(msg)
            if cnt != 0:
                all_passed = False

        if not all_passed:
            _log_and_print("One or more cleaning tests FAILED. See counts above.")
        else:
            _log_and_print("All cleaning tests PASSED")

    except Exception as e:
        err = f"An error occurred during cleaning: {e}"
        print(err)
        logger.exception(err)
    finally:
        if con is not None:
            con.close()

if __name__ == "__main__":
    clean_trips()
