import psycopg2
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import (
    StringType, StructType, StructField,
    LongType, ArrayType, MapType, BooleanType
)
from user_agents import parse as ua_parse
from datetime import datetime

# ─────────────────────────────────────────
# POSTGRES CONFIG
# ─────────────────────────────────────────
PG_CONN = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "postgres",
    "user":     "postgres",
    "password": "UnigapPostgres@123"
}

# ─────────────────────────────────────────
# KAFKA SCHEMA
# ─────────────────────────────────────────
glamira_schema = StructType([
    StructField("id",                  StringType(),  True),
    StructField("time_stamp",          LongType(),    True),
    StructField("ip",                  StringType(),  True),
    StructField("user_agent",          StringType(),  True),
    StructField("resolution",          StringType(),  True),
    StructField("device_id",           StringType(),  True),
    StructField("api_version",         StringType(),  True),
    StructField("store_id",            StringType(),  True),
    StructField("local_time",          StringType(),  True),
    StructField("show_recommendation", BooleanType(), True),
    StructField("current_url",         StringType(),  True),
    StructField("referrer_url",        StringType(),  True),
    StructField("email_address",       StringType(),  True),
    StructField("collection",          StringType(),  True),
    StructField("product_id",          StringType(),  True),
    StructField("option", ArrayType(
        MapType(StringType(), StringType())
    ), True),
])

# ─────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────
def get_browser(ua):
    try:
        return ua_parse(ua).browser.family
    except:
        return "Unknown"

def get_os(ua):
    try:
        return ua_parse(ua).os.family
    except:
        return "Unknown"

def get_device_type(ua):
    try:
        parsed = ua_parse(ua)
        if parsed.is_mobile:
            return "Mobile"
        elif parsed.is_tablet:
            return "Tablet"
        else:
            return "Desktop"
    except:
        return "Unknown"

def extract_domain(url):
    try:
        if not url:
            return None
        parsed = urlparse(url)
        return parsed.netloc.replace("www.", "")
    except:
        return None

def extract_country_from_domain(domain):
    # glamira.fr → fr, glamira.co.uk → uk, glamira.com → com
    try:
        if not domain:
            return None
        parts = domain.split(".")
        if len(parts) >= 2:
            return parts[-1].upper()
        return None
    except:
        return None

# ─────────────────────────────────────────
# UDFs
# ─────────────────────────────────────────
browser_udf      = udf(get_browser,     StringType())
os_udf           = udf(get_os,          StringType())
device_type_udf  = udf(get_device_type, StringType())
domain_udf       = udf(extract_domain,  StringType())

# ─────────────────────────────────────────
# DIM DEVICE
# ─────────────────────────────────────────
def process_dim_device(df):
    return df.select(
        browser_udf(col("user_agent")).alias("browser"),
        os_udf(col("user_agent")).alias("os"),
        device_type_udf(col("user_agent")).alias("device_type")
    ).distinct()

def store_dim_device(df):
    conn = psycopg2.connect(**PG_CONN)
    try:
        cursor = conn.cursor()
        for row in df.collect():
            cursor.execute("""
                INSERT INTO dim_device (browser, os, device_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (browser, os, device_type) DO NOTHING
            """, (row["browser"], row["os"], row["device_type"]))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────
# DIM DATE
# ─────────────────────────────────────────
def process_dim_date(df):
    rows = df.select("local_time").distinct().collect()
    dates = []
    for row in rows:
        try:
            dt = datetime.strptime(row["local_time"], "%Y-%m-%d %H:%M:%S")
            date_key = int(dt.strftime("%Y%m%d%H"))
            dates.append((date_key, dt.year, dt.month, dt.day, dt.hour))
        except:
            pass
    return dates

def store_dim_date(dates):
    conn = psycopg2.connect(**PG_CONN)
    try:
        cursor = conn.cursor()
        for (date_key, year, month, day, hour) in dates:
            cursor.execute("""
                INSERT INTO dim_date (date_key, year, month, day, hour)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO NOTHING
            """, (date_key, year, month, day, hour))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────
# DIM PRODUCT
# ─────────────────────────────────────────
def process_dim_product(df):
    return df.select("product_id") \
             .filter(col("product_id").isNotNull()) \
             .distinct()

def store_dim_product(df):
    conn = psycopg2.connect(**PG_CONN)
    try:
        cursor = conn.cursor()
        for row in df.collect():
            cursor.execute("""
                INSERT INTO dim_product (product_id)
                VALUES (%s)
                ON CONFLICT (product_id) DO NOTHING
            """, (row["product_id"],))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────
# DIM STORE
# ─────────────────────────────────────────
def process_dim_store(df):
    return df.select("store_id") \
             .filter(col("store_id").isNotNull()) \
             .distinct()

def store_dim_store(df):
    conn = psycopg2.connect(**PG_CONN)
    try:
        cursor = conn.cursor()
        for row in df.collect():
            cursor.execute("""
                INSERT INTO dim_store (store_id)
                VALUES (%s)
                ON CONFLICT (store_id) DO NOTHING
            """, (row["store_id"],))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────
# DIM REFERRER
# ─────────────────────────────────────────
def process_dim_referrer(df):
    return df.select(
        domain_udf(col("referrer_url")).alias("referrer_domain")
    ).filter(col("referrer_domain").isNotNull()) \
     .distinct()

def store_dim_referrer(df):
    conn = psycopg2.connect(**PG_CONN)
    try:
        cursor = conn.cursor()
        for row in df.collect():
            cursor.execute("""
                INSERT INTO dim_referrer (referrer_domain)
                VALUES (%s)
                ON CONFLICT (referrer_domain) DO NOTHING
            """, (row["referrer_domain"],))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────
# DIM LOCATION
# ─────────────────────────────────────────
def process_dim_location(df):
    return df.select(
        col("ip").alias("ip_address"),
        domain_udf(col("current_url")).alias("domain")
    ).filter(col("ip_address").isNotNull()) \
     .distinct()

def store_dim_location(df):
    conn = psycopg2.connect(**PG_CONN)
    try:
        cursor = conn.cursor()
        for row in df.collect():
            domain       = row["domain"]
            country_code = extract_country_from_domain(domain)
            cursor.execute("""
                INSERT INTO dim_location (ip_address, country_code, domain)
                VALUES (%s, %s, %s)
                ON CONFLICT (ip_address) DO NOTHING
            """, (row["ip_address"], country_code, domain))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────
# FACT TABLE
# ─────────────────────────────────────────
def store_fact(df):
    conn = psycopg2.connect(**PG_CONN)
    try:
        cursor = conn.cursor()
        for row in df.collect():

            # look up device_key
            cursor.execute("""
                SELECT device_key FROM dim_device
                WHERE browser = %s AND os = %s AND device_type = %s
            """, (
                get_browser(row["user_agent"]),
                get_os(row["user_agent"]),
                get_device_type(row["user_agent"])
            ))
            result     = cursor.fetchone()
            device_key = result[0] if result else None

            # look up date_key
            date_key   = None
            event_ts   = None
            event_hour = None
            try:
                dt         = datetime.strptime(row["local_time"], "%Y-%m-%d %H:%M:%S")
                date_key   = int(dt.strftime("%Y%m%d%H"))
                event_ts   = dt
                event_hour = dt.hour
            except:
                pass

            # look up product_key
            cursor.execute("""
                SELECT product_key FROM dim_product
                WHERE product_id = %s
            """, (row["product_id"],))
            result      = cursor.fetchone()
            product_key = result[0] if result else None

            # look up store_key
            cursor.execute("""
                SELECT store_key FROM dim_store
                WHERE store_id = %s
            """, (row["store_id"],))
            result    = cursor.fetchone()
            store_key = result[0] if result else None

            # look up location_key
            cursor.execute("""
                SELECT location_key FROM dim_location
                WHERE ip_address = %s
            """, (row["ip"],))
            result       = cursor.fetchone()
            location_key = result[0] if result else None

            # look up referrer_key
            referrer_domain = extract_domain(row["referrer_url"])
            cursor.execute("""
                SELECT referrer_key FROM dim_referrer
                WHERE referrer_domain = %s
            """, (referrer_domain,))
            result       = cursor.fetchone()
            referrer_key = result[0] if result else None

            cursor.execute("""
                INSERT INTO fact_product_view (
                    view_id, date_key, product_key, store_key,
                    location_key, device_key, referrer_key,
                    event_ts, event_hour, time_stamp,
                    current_url, referrer_url, ip_address,
                    collection, api_version, ingested_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (view_id) DO NOTHING
            """, (
                row["id"],
                date_key,
                product_key,
                store_key,
                location_key,
                device_key,
                referrer_key,
                event_ts,
                event_hour,
                row["time_stamp"],
                row["current_url"],
                row["referrer_url"],
                row["ip"],
                row["collection"],
                row["api_version"],
                datetime.utcnow()
            ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────
# FOREACH BATCH
# ─────────────────────────────────────────
def process_batch(df, batch_id):
    print(f"Processing batch {batch_id}")

    parsed_df = df.select(
        from_json(
            col("value").cast(StringType()),
            glamira_schema
        ).alias("data")
    ).select("data.*")

    parsed_df.cache()

    # dims first
    store_dim_device(process_dim_device(parsed_df))
    store_dim_date(process_dim_date(parsed_df))
    store_dim_product(process_dim_product(parsed_df))
    store_dim_store(process_dim_store(parsed_df))
    store_dim_referrer(process_dim_referrer(parsed_df))
    store_dim_location(process_dim_location(parsed_df))

    # fact last — all keys are now available
    store_fact(parsed_df)

    parsed_df.unpersist()
    print(f"Batch {batch_id} done")

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("GlamiraKafkaStreaming") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                "46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294") \
        .option("subscribe", "product_view") \
        .option("startingOffsets", "latest") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",
                'org.apache.kafka.common.security.plain.PlainLoginModule required '
                'username="kafka" password="UnigapKafka@2024";') \
        .load()

    query = kafka_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoint/glamira") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()
