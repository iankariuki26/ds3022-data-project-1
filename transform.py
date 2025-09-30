import duckdb
import logging
from pathlib import Path



DB_PATH = "nytaxi.db"   
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / "transform.log"

#once again for logging
logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("transform")


#adding emissions table and derived fields and write it all to a trips_2024_final table. Had some help from chat 
def transform():
    con = None
    try:
        con = duckdb.connect(database=DB_PATH, read_only=False)

        

        con.execute("""
            CREATE TABLE IF NOT EXISTS emissions (
                service TEXT PRIMARY KEY,
                kg_co2_per_mile DOUBLE
            );
        """)
        # Idempotent refresh
        con.execute("DROP TABLE IF EXISTS emissions;")
        con.execute("""
            CREATE TABLE emissions AS
            SELECT 'yellow' AS service, 0.404 AS kg_co2_per_mile
            UNION ALL
            SELECT 'green'  AS service, 0.350 AS kg_co2_per_mile;
            """)
        
        log.info("Created emissions table: yellow=0.404, green=0.350 kg/mi")
        log.info("Upserted emissions factors (kg CO2 per mile): yellow=0.404, green=0.350")

        #had some help from chat here
        con.execute("DROP TABLE IF EXISTS trips_2024_final;")
        con.execute("""
            CREATE TABLE trips_2024_final AS
            SELECT
            t.service,
            t.pickup_datetime,
            t.dropoff_datetime,
            t.passenger_count,
            t.trip_distance,

            /* duration in hours */
            CAST(date_diff('minute', t.pickup_datetime, t.dropoff_datetime) AS DOUBLE) / 60.0
                AS duration_hours,

            /* avg mph: protect against division by zero */
            CASE
                WHEN date_diff('minute', t.pickup_datetime, t.dropoff_datetime) > 0
                THEN t.trip_distance / (CAST(date_diff('minute', t.pickup_datetime, t.dropoff_datetime) AS DOUBLE) / 60.0)
            END AS avg_mph,

            /* CO2 per trip */
            (t.trip_distance * e.kg_co2_per_mile) AS co2_kg,

            /* calendar features */
            EXTRACT(HOUR  FROM t.pickup_datetime) AS trip_hour,
            dayname(t.pickup_datetime)            AS trip_day_of_week,
            week(t.pickup_datetime)               AS week_number,
            month(t.pickup_datetime)              AS month_number

            FROM trips_2024 t
            LEFT JOIN emissions e
            ON e.service = t.service;
            """)
        log.info("Built trips_2024_final with derived fields.")

        counts = con.execute("SELECT COUNT(*) FROM trips_2024_final").fetchone()[0]
        co2 = con.execute("SELECT ROUND(SUM(co2_kg), 2) FROM trips_2024_final").fetchone()[0]
        log.info("trips_2024_final rows = %s, total CO2 (kg) = %s", counts, co2)
        print(f"Built trips_2024_final — rows: {counts:,} | total CO2 (kg): {co2}")

    except Exception as e:
        log.exception("Transform failed: %s", e)
        print(f"Transform failed: {e}")
    finally:
        if con:
            con.close()
            log.info("Closed connection to %s", DB_PATH)

if __name__ == "__main__":
    transform()
