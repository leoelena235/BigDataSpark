from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("ETL_Postgres_to_Star")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)

pg_url = "jdbc:postgresql://lab2_postgres:5432/lab2_db"
props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

src = spark.read.jdbc(pg_url, "public.mock_data", properties=props)

string_cols = [
    "customer_country",
    "customer_pet_type",
    "customer_pet_name",
    "customer_pet_breed",
    "seller_first_name",
    "seller_last_name",
    "seller_email",
    "seller_country",
    "seller_postal_code",
    "product_name",
    "product_category",
    "pet_category",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email",
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country",
]

for c in string_cols:
    src = src.withColumn(c, F.trim(F.col(c)))
    src = src.withColumn(c, F.when(F.col(c) == "", F.lit(None)).otherwise(F.col(c)))


def add_sk(df, order_cols, sk_name):
    w = Window.orderBy(
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in order_cols]
    )
    return df.withColumn(sk_name, F.row_number().over(w))


def write_table(df, table_name):
    (
        df.write.mode("overwrite")
        .option("truncate", "true")
        .jdbc(pg_url, table_name, properties=props)
    )


dim_country = (
    src.select(
        F.explode(
            F.array(
                F.col("customer_country"),
                F.col("seller_country"),
                F.col("store_country"),
                F.col("supplier_country"),
            )
        ).alias("country_name")
    )
    .where(F.col("country_name").isNotNull() & (F.col("country_name") != ""))
    .dropDuplicates(["country_name"])
)

dim_country = add_sk(dim_country, ["country_name"], "country_sk").select(
    "country_sk", "country_name"
)

dim_pet = src.select(
    F.coalesce(F.col("customer_pet_type"), F.lit("Unknown")).alias("pet_type"),
    F.coalesce(F.col("customer_pet_name"), F.lit("Unknown")).alias("pet_name"),
    F.coalesce(F.col("customer_pet_breed"), F.lit("Unknown")).alias("pet_breed"),
).dropDuplicates()

dim_pet = add_sk(dim_pet, ["pet_type", "pet_name", "pet_breed"], "pet_sk").select(
    "pet_sk", "pet_type", "pet_name", "pet_breed"
)

dim_product_category = src.select(
    F.coalesce(F.col("product_category"), F.lit("Unknown")).alias(
        "product_category_name"
    )
).dropDuplicates()

dim_product_category = add_sk(
    dim_product_category, ["product_category_name"], "product_category_sk"
).select("product_category_sk", "product_category_name")

dim_pet_category = src.select(
    F.coalesce(F.col("pet_category"), F.lit("Unknown")).alias("pet_category_name")
).dropDuplicates()

dim_pet_category = add_sk(
    dim_pet_category, ["pet_category_name"], "pet_category_sk"
).select("pet_category_sk", "pet_category_name")

dim_customer_nat = (
    src.select(
        F.col("sale_customer_id").cast("int").alias("source_customer_id"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_age").cast("int").alias("age"),
        F.col("customer_email").alias("email"),
        F.col("customer_postal_code").alias("postal_code"),
        F.coalesce(F.col("customer_country"), F.lit("Unknown")).alias("country_name"),
        F.coalesce(F.col("customer_pet_type"), F.lit("Unknown")).alias("pet_type"),
        F.coalesce(F.col("customer_pet_name"), F.lit("Unknown")).alias("pet_name"),
        F.coalesce(F.col("customer_pet_breed"), F.lit("Unknown")).alias("pet_breed"),
    )
    .dropDuplicates(["source_customer_id"])
    .join(dim_country, "country_name", "left")
    .join(dim_pet, ["pet_type", "pet_name", "pet_breed"], "left")
)

dim_customer = add_sk(dim_customer_nat, ["source_customer_id"], "customer_sk").select(
    "customer_sk",
    "source_customer_id",
    "first_name",
    "last_name",
    "age",
    "email",
    "postal_code",
    "country_sk",
    "pet_sk",
)

dim_seller_nat = (
    src.select(
        F.col("sale_seller_id").cast("int").alias("source_seller_id"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_postal_code").alias("postal_code"),
        F.coalesce(F.col("seller_country"), F.lit("Unknown")).alias("country_name"),
    )
    .dropDuplicates(["source_seller_id"])
    .join(dim_country, "country_name", "left")
)

dim_seller = add_sk(dim_seller_nat, ["source_seller_id"], "seller_sk").select(
    "seller_sk",
    "source_seller_id",
    "first_name",
    "last_name",
    "email",
    "postal_code",
    "country_sk",
)

dim_supplier_nat = (
    src.select(
        F.col("supplier_name").alias("supplier_name"),
        F.col("supplier_contact").alias("contact_person"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.col("supplier_city").alias("city"),
        F.coalesce(F.col("supplier_country"), F.lit("Unknown")).alias("country_name"),
    )
    .dropDuplicates(["email"])
    .join(dim_country, "country_name", "left")
)

dim_supplier = add_sk(dim_supplier_nat, ["email"], "supplier_sk").select(
    "supplier_sk",
    "supplier_name",
    "contact_person",
    "email",
    "phone",
    "address",
    "city",
    "country_sk",
)

dim_store_nat = (
    src.select(
        F.col("store_name").alias("store_name"),
        F.col("store_location").alias("location"),
        F.col("store_city").alias("city"),
        F.col("store_state").alias("state"),
        F.coalesce(F.col("store_country"), F.lit("Unknown")).alias("country_name"),
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email"),
    )
    .dropDuplicates()
    .join(dim_country, "country_name", "left")
)

dim_store = add_sk(
    dim_store_nat,
    ["store_name", "location", "city", "state", "phone", "email"],
    "store_sk",
).select(
    "store_sk",
    "store_name",
    "location",
    "city",
    "state",
    "country_sk",
    "phone",
    "email",
)

dim_product_nat = (
    src.select(
        F.col("sale_product_id").cast("int").alias("source_product_id"),
        F.col("product_name"),
        F.coalesce(F.col("product_category"), F.lit("Unknown")).alias(
            "product_category_name"
        ),
        F.coalesce(F.col("pet_category"), F.lit("Unknown")).alias("pet_category_name"),
        F.col("product_price").cast("decimal(10,2)").alias("product_price"),
        F.col("product_quantity").cast("int").alias("product_quantity"),
        F.col("product_weight").cast("decimal(10,2)").alias("product_weight"),
        F.col("product_color"),
        F.col("product_size"),
        F.col("product_brand"),
        F.col("product_material"),
        F.col("product_description"),
        F.col("product_rating").cast("decimal(3,2)").alias("product_rating"),
        F.col("product_reviews").cast("int").alias("product_reviews"),
        F.col("product_release_date"),
        F.col("product_expiry_date"),
    )
    .dropDuplicates(["source_product_id"])
    .join(dim_product_category, "product_category_name", "left")
    .join(dim_pet_category, "pet_category_name", "left")
)

dim_product = add_sk(dim_product_nat, ["source_product_id"], "product_sk").select(
    "product_sk",
    "source_product_id",
    "product_name",
    "product_category_sk",
    "pet_category_sk",
    "product_price",
    "product_quantity",
    "product_weight",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "product_rating",
    "product_reviews",
    "product_release_date",
    "product_expiry_date",
)

dim_date = (
    src.select(F.col("sale_date").cast("date").alias("full_date"))
    .dropDuplicates(["full_date"])
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("month_num", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("quarter_num", F.quarter("full_date"))
    .withColumn("year_num", F.year("full_date"))
    .withColumn("day_of_week_num", F.date_format("full_date", "u").cast("int"))
    .withColumn("day_of_week_name", F.date_format("full_date", "EEEE"))
)

dim_date = add_sk(dim_date, ["full_date"], "date_sk").select(
    "date_sk",
    "full_date",
    "day_of_month",
    "month_num",
    "month_name",
    "quarter_num",
    "year_num",
    "day_of_week_num",
    "day_of_week_name",
)

sale_src = src.select(
    F.col("id").cast("int").alias("source_id"),
    F.col("sale_date").cast("date").alias("full_date"),
    F.col("sale_customer_id").cast("int").alias("source_customer_id"),
    F.col("sale_seller_id").cast("int").alias("source_seller_id"),
    F.col("sale_product_id").cast("int").alias("source_product_id"),
    F.col("sale_quantity").cast("int").alias("sale_quantity"),
    F.col("sale_total_price").cast("decimal(10,2)").alias("sale_total_price"),
    F.col("store_name"),
    F.col("store_location").alias("location"),
    F.col("store_city").alias("city"),
    F.col("store_state").alias("state"),
    F.col("store_phone").alias("phone"),
    F.col("store_email").alias("email"),
    F.col("supplier_name"),
    F.col("supplier_contact").alias("contact_person"),
    F.col("supplier_email").alias("supplier_email"),
    F.col("supplier_phone").alias("supplier_phone"),
    F.col("supplier_address").alias("supplier_address"),
    F.col("supplier_city").alias("supplier_city"),
)

store_join = dim_store.select(
    F.col("store_sk"),
    F.col("store_name"),
    F.col("location"),
    F.col("city"),
    F.col("state"),
    F.col("phone"),
    F.col("email"),
).alias("st")

supplier_join = dim_supplier.select(
    F.col("supplier_sk"),
    F.col("supplier_name").alias("sup_supplier_name"),
    F.col("contact_person").alias("sup_contact_person"),
    F.col("email").alias("sup_email"),
    F.col("phone").alias("sup_phone"),
    F.col("address").alias("sup_address"),
    F.col("city").alias("sup_city"),
).alias("sup")

fact_sale = (
    sale_src.alias("src")
    .join(
        dim_date.select("date_sk", "full_date").alias("d"),
        F.col("src.full_date") == F.col("d.full_date"),
        "left",
    )
    .join(
        dim_customer.select("customer_sk", "source_customer_id").alias("c"),
        F.col("src.source_customer_id") == F.col("c.source_customer_id"),
        "left",
    )
    .join(
        dim_seller.select("seller_sk", "source_seller_id").alias("s"),
        F.col("src.source_seller_id") == F.col("s.source_seller_id"),
        "left",
    )
    .join(
        dim_product.select("product_sk", "source_product_id").alias("p"),
        F.col("src.source_product_id") == F.col("p.source_product_id"),
        "left",
    )
    .join(
        store_join,
        (F.col("src.store_name") == F.col("st.store_name"))
        & (F.col("src.location") == F.col("st.location"))
        & (F.col("src.city") == F.col("st.city"))
        & (F.col("src.state") == F.col("st.state"))
        & (F.col("src.phone") == F.col("st.phone"))
        & (F.col("src.email") == F.col("st.email")),
        "left",
    )
    .join(
        supplier_join,
        (F.col("src.supplier_name") == F.col("sup.sup_supplier_name"))
        & (F.col("src.contact_person") == F.col("sup.sup_contact_person"))
        & (F.col("src.supplier_email") == F.col("sup.sup_email"))
        & (F.col("src.supplier_phone") == F.col("sup.sup_phone"))
        & (F.col("src.supplier_address") == F.col("sup.sup_address"))
        & (F.col("src.supplier_city") == F.col("sup.sup_city")),
        "left",
    )
    .select(
        F.col("src.source_id").alias("source_id"),
        F.col("d.date_sk").alias("date_sk"),
        F.col("c.customer_sk").alias("customer_sk"),
        F.col("s.seller_sk").alias("seller_sk"),
        F.col("p.product_sk").alias("product_sk"),
        F.col("st.store_sk").alias("store_sk"),
        F.col("sup.supplier_sk").alias("supplier_sk"),
        F.col("src.sale_quantity").alias("sale_quantity"),
        F.col("src.sale_total_price").alias("sale_total_price"),
    )
)

fact_sale = add_sk(fact_sale, ["source_id"], "sale_sk").select(
    "sale_sk",
    "source_id",
    "date_sk",
    "customer_sk",
    "seller_sk",
    "product_sk",
    "store_sk",
    "supplier_sk",
    "sale_quantity",
    "sale_total_price",
)

for df, table in [
    (dim_country, "dim_country"),
    (dim_pet, "dim_pet"),
    (dim_product_category, "dim_product_category"),
    (dim_pet_category, "dim_pet_category"),
    (dim_customer, "dim_customer"),
    (dim_seller, "dim_seller"),
    (dim_supplier, "dim_supplier"),
    (dim_store, "dim_store"),
    (dim_product, "dim_product"),
    (dim_date, "dim_date"),
    (fact_sale, "fact_sale"),
]:
    write_table(df, table)

print("ETL завершен")
spark.stop()
