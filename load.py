import os
import time
import logging
import duckdb


#Was going to do the decade with the help of a ec2 instance while trying to implement s3,IAM, and prefect but ran into an unholy amount of 
# issues throughout the last few days which and was running out of time so I 
# just had to restart last minute and go for simply the year of 2024. Didn't have much time and had to sometimes get quick help 
# from chat when i ran into issues here



# this is for the logger, had chat help with the latter half of this
def get_logger(name="ingest", log_dir="logs", filename="load.log"):
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:  # prevent duplicate handlers on re-runs
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        fh = logging.FileHandler(os.path.join(log_dir, filename))
        fh.setFormatter(fmt)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger

log = get_logger(name="ingest", filename="ingest_load.log")

link_head = "https://d37ci6vzurychx.cloudfront.net/trip-data"

datasets = {
    "yellow": "yellow_tripdata_{y}-{m:02d}.parquet",
    "green": "green_tripdata_{y}-{m:02d}.parquet"
}


#this is for building a list of the the nytaxi parquet URLs for the given years/months/colors.
def make_urls(years, months, color):
    urls = []
    for y in years:
        for m in months:
            for c in color:
                fname = datasets[c].format(y=y, m=m)
                urls.append(f"{link_head}/{fname}")
    log.info("Prepared %d URLs (years=%s, months=%s, colors=%s)", len(urls), years, months, color)
    # Optional: show a few examples to the log
    if urls:
        log.info("Example URL(s): %s", ", ".join(urls[:3]) + (" ..." if len(urls) > 3 else ""))
    return urls


#This is for creating a DuckDB table, loading in the first file, and then append remaining files with a bit of sleep to prevent throttling
def load_table(urls, db_path, table_name):
    start = time.time()
    log.info("Loading table '%s' into %s from %d parquet file(s)", table_name, db_path, len(urls))
    con = None
    try:
        con = duckdb.connect(database=db_path, read_only=False)
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET unsafe_disable_etag_checks = true;")

        
        con.execute(f"""
                    DROP TABLE IF EXISTS {table_name};       
                    CREATE TABLE {table_name} 
                    AS
                    SELECT * 
                    FROM read_parquet($first_file, union_by_name=true,filename=true);
                    """, {"first_file":urls[0]}
                    )
        print(f"created '{table_name}' from: {urls[0]}")
        

        for i in range(1,len(urls)):

            con.execute(f"""
                    INSERT INTO {table_name} SELECT * FROM read_parquet("{urls[i]}", union_by_name=true, filename=true);
                    """
                    )
            time.sleep(5)

        # Log resulting row count
        
        rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        elapsed = time.time() - start
        log.info("Created table '%s' with %d rows from %d file(s) in %.2fs",
                 table_name, rows, len(urls), elapsed)
    except Exception as e:
        log.exception("Failed to load table '%s' from %d file(s): %s", table_name, len(urls), e)
        raise
    finally:
        if con is not None:
            con.close()
            log.info("Closed connection to %s", db_path)



if __name__ == "__main__":
    years = [2024]
    months = list(range(1, 13))

    yellow_urls = make_urls(years, months, ["yellow"])
    green_urls = make_urls(years, months, ["green"])

    load_table(yellow_urls, "nytaxi.db", "yellow_taxi")
    load_table(green_urls, "nytaxi.db", "green_taxi")
