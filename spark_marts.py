from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType
import clickhouse_connect

spark = (
    SparkSession.builder.appName("Build_Marts_ClickHouse")
    .config("spark.jars", "/opt/spark/jars/postgresql.jar")
    .getOrCreate()
)

pg_url = "jdbc:postgresql://lab2_postgres:5432/lab2_db"
props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

fact = spark.read.jdbc(pg_url, "fact_sale", properties=props)
dim_product = spark.read.jdbc(pg_url, "dim_product", properties=props)
dim_customer = spark.read.jdbc(pg_url, "dim_customer", properties=props)
dim_seller = spark.read.jdbc(pg_url, "dim_seller", properties=props)
dim_store = spark.read.jdbc(pg_url, "dim_store", properties=props)
dim_supplier = spark.read.jdbc(pg_url, "dim_supplier", properties=props)
dim_date = spark.read.jdbc(pg_url, "dim_date", properties=props)
dim_country = spark.read.jdbc(pg_url, "dim_country", properties=props)
dim_product_category = spark.read.jdbc(pg_url, "dim_product_category", properties=props)
dim_pet_category = spark.read.jdbc(pg_url, "dim_pet_category", properties=props)

client = clickhouse_connect.get_client(
    host="lab2_clickhouse", port=8123, username="default", password=""
)


def ch_type(dt, nullable=True):
    t = dt.typeName()
    base = "String"
    if t in ("int", "integer", "short", "byte"):
        base = "Int32"
    elif t in ("long", "bigint"):
        base = "Int64"
    elif t in ("double",):
        base = "Float64"
    elif t in ("float",):
        base = "Float32"
    elif t in ("date",):
        base = "Date"
    elif t in ("timestamp",):
        base = "DateTime"
    elif "decimal" in t:
        base = "Decimal(18,2)"
    return f"Nullable({base})" if nullable else base


def ensure_table(df, table_name, order_by="tuple()"):
    cols = []
    for f in df.schema.fields:
        cols.append(f"{f.name} {ch_type(f.dataType, True)}")
    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)}) ENGINE = MergeTree() ORDER BY {order_by}"
    client.command(ddl)


def write_ch(df, table_name, order_by="tuple()"):
    ensure_table(df, table_name, order_by)
    rows = [
        tuple(None if (isinstance(v, float) and v != v) else v for v in r)
        for r in df.collect()
    ]
    if rows:
        client.insert(table_name, rows, column_names=df.columns)


product_metrics = (
    fact.groupBy("product_sk")
    .agg(
        F.sum("sale_total_price").alias("revenue"),
        F.sum("sale_quantity").alias("sold_qty"),
    )
    .join(
        dim_product.select(
            "product_sk", "source_product_id", "product_name", "product_category_sk"
        ),
        "product_sk",
        "left",
    )
    .join(dim_product_category, "product_category_sk", "left")
    .join(
        dim_product.select("product_sk", "product_rating", "product_reviews"),
        "product_sk",
        "left",
    )
)

top_products = (
    product_metrics.orderBy(F.desc("revenue"))
    .limit(10)
    .select("product_name", "product_category_name", "revenue", "sold_qty")
)

category_revenue = (
    product_metrics.groupBy("product_category_name")
    .agg(F.sum("revenue").alias("total_revenue"))
    .orderBy(F.desc("total_revenue"))
)

product_rating_reviews = dim_product.select(
    F.col("product_name"), F.col("product_rating"), F.col("product_reviews")
).orderBy(F.desc("product_rating"))

customer_metrics = (
    fact.groupBy("customer_sk")
    .agg(
        F.sum("sale_total_price").alias("total_spent"),
        F.count("*").alias("order_count"),
        F.avg("sale_total_price").alias("avg_check"),
    )
    .join(
        dim_customer.select(
            "customer_sk", "source_customer_id", "first_name", "last_name", "country_sk"
        ),
        "customer_sk",
        "left",
    )
    .join(dim_country, "country_sk", "left")
)

top_customers = (
    customer_metrics.orderBy(F.desc("total_spent"))
    .limit(10)
    .select(
        "first_name",
        "last_name",
        "country_name",
        "total_spent",
        "order_count",
        "avg_check",
    )
)

customer_country_dist = (
    dim_customer.join(dim_country, "country_sk", "left")
    .groupBy("country_name")
    .agg(F.count("*").alias("customers_count"))
    .orderBy(F.desc("customers_count"))
)

customer_avg_check = customer_metrics.select(
    "source_customer_id", "first_name", "last_name", "avg_check", "order_count"
)

time_metrics = (
    fact.join(dim_date, "date_sk", "left")
    .groupBy("year_num", "month_num", "month_name")
    .agg(
        F.sum("sale_total_price").alias("monthly_revenue"),
        F.avg("sale_total_price").alias("avg_order_value"),
        F.avg("sale_quantity").alias("avg_order_qty"),
    )
    .orderBy("year_num", "month_num")
)

yearly_revenue = (
    fact.join(dim_date, "date_sk", "left")
    .groupBy("year_num")
    .agg(F.sum("sale_total_price").alias("yearly_revenue"))
    .orderBy("year_num")
)

yearly_compare = (
    yearly_revenue.alias("a")
    .join(
        yearly_revenue.select(
            F.col("year_num").alias("prev_year"),
            F.col("yearly_revenue").alias("prev_year_revenue"),
        ).alias("b"),
        F.col("a.year_num") == F.col("b.prev_year") + 1,
        "left",
    )
    .select(
        F.col("a.year_num").alias("year_num"),
        F.col("a.yearly_revenue"),
        F.col("b.prev_year_revenue"),
        F.round(
            (F.col("a.yearly_revenue") - F.col("b.prev_year_revenue"))
            / F.col("b.prev_year_revenue")
            * 100,
            2,
        ).alias("growth_pct"),
    )
)

avg_order_month = time_metrics.select(
    "year_num", "month_num", "month_name", "avg_order_value", "avg_order_qty"
)

store_metrics = (
    fact.groupBy("store_sk")
    .agg(
        F.sum("sale_total_price").alias("revenue"),
        F.avg("sale_total_price").alias("avg_check"),
    )
    .join(
        dim_store.select("store_sk", "store_name", "city", "country_sk"),
        "store_sk",
        "left",
    )
    .join(dim_country, "country_sk", "left")
)

top_stores = (
    store_metrics.orderBy(F.desc("revenue"))
    .limit(5)
    .select("store_name", "city", "country_name", "revenue", "avg_check")
)

store_city_country = (
    dim_store.join(dim_country, "country_sk", "left")
    .groupBy("city", "country_name")
    .agg(F.count("*").alias("stores_count"))
    .orderBy(F.desc("stores_count"))
)

store_avg_check = store_metrics.select("store_name", "avg_check", "revenue")

supplier_metrics = (
    fact.groupBy("supplier_sk")
    .agg(F.sum("sale_total_price").alias("revenue"))
    .join(
        dim_supplier.select("supplier_sk", "supplier_name", "country_sk"),
        "supplier_sk",
        "left",
    )
    .join(dim_country, "country_sk", "left")
)

top_suppliers = (
    supplier_metrics.orderBy(F.desc("revenue"))
    .limit(5)
    .select("supplier_name", "country_name", "revenue")
)

supplier_avg_price = (
    fact.join(dim_product.select("product_sk", "product_price"), "product_sk", "left")
    .join(
        dim_supplier.select("supplier_sk", "supplier_name", "country_sk"),
        "supplier_sk",
        "left",
    )
    .groupBy("supplier_name", "country_sk")
    .agg(F.avg("product_price").alias("avg_product_price"))
    .join(dim_country, "country_sk", "left")
    .select("supplier_name", "country_name", "avg_product_price")
)

supplier_country_dist = (
    dim_supplier.join(dim_country, "country_sk", "left")
    .groupBy("country_name")
    .agg(F.count("*").alias("suppliers_count"))
    .orderBy(F.desc("suppliers_count"))
)

product_sales_qty = (
    fact.groupBy("product_sk")
    .agg(F.sum("sale_quantity").alias("total_sold"))
    .join(
        dim_product.select(
            "product_sk", "product_name", "product_rating", "product_reviews"
        ),
        "product_sk",
        "left",
    )
)

highest_rated = (
    product_sales_qty.orderBy(F.desc("product_rating"))
    .limit(5)
    .select("product_name", "product_rating", "total_sold", "product_reviews")
)

lowest_rated = (
    product_sales_qty.orderBy(F.asc("product_rating"))
    .limit(5)
    .select("product_name", "product_rating", "total_sold", "product_reviews")
)

most_reviewed = (
    product_sales_qty.orderBy(F.desc("product_reviews"))
    .limit(5)
    .select("product_name", "product_reviews", "product_rating", "total_sold")
)

corr = product_sales_qty.select(
    F.col("product_rating").cast("double").alias("rating"),
    F.col("total_sold").cast("double").alias("sold"),
).stat.corr("rating", "sold")

corr_df = spark.createDataFrame(
    [(float(corr) if corr is not None else 0.0,)], ["rating_sales_correlation"]
)

tables = [
    (top_products, "mart_top_products", "tuple()"),
    (category_revenue, "mart_category_revenue", "tuple()"),
    (product_rating_reviews, "mart_product_rating_reviews", "tuple()"),
    (top_customers, "mart_top_customers", "tuple()"),
    (customer_country_dist, "mart_customer_country_dist", "tuple()"),
    (customer_avg_check, "mart_customer_avg_check", "tuple()"),
    (time_metrics, "mart_monthly_trend", "tuple()"),
    (yearly_revenue, "mart_yearly_revenue", "tuple()"),
    (yearly_compare, "mart_yearly_compare", "tuple()"),
    (avg_order_month, "mart_avg_order_month", "tuple()"),
    (top_stores, "mart_top_stores", "tuple()"),
    (store_city_country, "mart_store_city_country", "tuple()"),
    (store_avg_check, "mart_store_avg_check", "tuple()"),
    (top_suppliers, "mart_top_suppliers", "tuple()"),
    (supplier_avg_price, "mart_supplier_avg_price", "tuple()"),
    (supplier_country_dist, "mart_supplier_country_dist", "tuple()"),
    (highest_rated, "mart_highest_rated", "tuple()"),
    (lowest_rated, "mart_lowest_rated", "tuple()"),
    (most_reviewed, "mart_most_reviewed", "tuple()"),
    (corr_df, "mart_rating_sales_correlation", "tuple()"),
]

for df, name, order_by in tables:
    write_ch(df, name, order_by)

print("Витрины готовы")
spark.stop()
