CREATE TABLE dim_country (
    country_sk   SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_pet (
    pet_sk    SERIAL PRIMARY KEY,
    pet_type  VARCHAR(50),
    pet_name  VARCHAR(100),
    pet_breed VARCHAR(100),
    CONSTRAINT uq_dim_pet UNIQUE (pet_type, pet_name, pet_breed)
);

CREATE TABLE dim_customer (
    customer_sk        SERIAL PRIMARY KEY,
    source_customer_id INTEGER NOT NULL UNIQUE,
    first_name         VARCHAR(50),
    last_name          VARCHAR(50),
    age                INTEGER,
    email              VARCHAR(255),
    postal_code        VARCHAR(20),
    country_sk         INTEGER REFERENCES dim_country(country_sk),
    pet_sk             INTEGER REFERENCES dim_pet(pet_sk)
);

CREATE TABLE dim_seller (
    seller_sk          SERIAL PRIMARY KEY,
    source_seller_id   INTEGER NOT NULL UNIQUE,
    first_name         VARCHAR(50),
    last_name          VARCHAR(50),
    email              VARCHAR(255),
    postal_code        VARCHAR(20),
    country_sk         INTEGER REFERENCES dim_country(country_sk)
);

CREATE TABLE dim_supplier (
    supplier_sk      SERIAL PRIMARY KEY,
    supplier_name    VARCHAR(255),
    contact_person   VARCHAR(255),
    email            VARCHAR(255),
    phone            VARCHAR(50),
    address          VARCHAR(255),
    city             VARCHAR(100),
    country_sk       INTEGER REFERENCES dim_country(country_sk),
    CONSTRAINT uq_dim_supplier_email UNIQUE (email)
);

CREATE TABLE dim_store (
    store_sk      SERIAL PRIMARY KEY,
    store_name    VARCHAR(255),
    location      VARCHAR(255),
    city          VARCHAR(100),
    state         VARCHAR(100),
    country_sk    INTEGER REFERENCES dim_country(country_sk),
    phone         VARCHAR(50),
    email         VARCHAR(255),
    CONSTRAINT uq_dim_store UNIQUE (store_name, location, city, state, phone, email)
);

CREATE TABLE dim_product_category (
    product_category_sk   SERIAL PRIMARY KEY,
    product_category_name  VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_pet_category (
    pet_category_sk   SERIAL PRIMARY KEY,
    pet_category_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_product (
    product_sk          SERIAL PRIMARY KEY,
    source_product_id   INTEGER NOT NULL UNIQUE,
    product_name        VARCHAR(255),
    product_category_sk INTEGER REFERENCES dim_product_category(product_category_sk),
    pet_category_sk     INTEGER REFERENCES dim_pet_category(pet_category_sk),
    product_price       NUMERIC(10,2),
    product_quantity    INTEGER,
    product_weight      NUMERIC(10,2),
    product_color       VARCHAR(50),
    product_size        VARCHAR(50),
    product_brand       VARCHAR(100),
    product_material    VARCHAR(100),
    product_description TEXT,
    product_rating      NUMERIC(3,2),
    product_reviews     INTEGER,
    product_release_date DATE,
    product_expiry_date  DATE
);

CREATE TABLE dim_date (
    date_sk          SERIAL PRIMARY KEY,
    full_date        DATE NOT NULL UNIQUE,
    day_of_month     SMALLINT NOT NULL,
    month_num        SMALLINT NOT NULL,
    month_name       VARCHAR(20) NOT NULL,
    quarter_num      SMALLINT NOT NULL,
    year_num         INTEGER NOT NULL,
    day_of_week_num  SMALLINT NOT NULL,
    day_of_week_name VARCHAR(20) NOT NULL
);

CREATE TABLE fact_sale (
    sale_sk          SERIAL PRIMARY KEY,
    source_id        INTEGER NOT NULL,
    date_sk          INTEGER REFERENCES dim_date(date_sk),
    customer_sk      INTEGER REFERENCES dim_customer(customer_sk),
    seller_sk        INTEGER REFERENCES dim_seller(seller_sk),
    product_sk       INTEGER REFERENCES dim_product(product_sk),
    store_sk         INTEGER REFERENCES dim_store(store_sk),
    supplier_sk      INTEGER REFERENCES dim_supplier(supplier_sk),
    sale_quantity    INTEGER NOT NULL,
    sale_total_price NUMERIC(10,2) NOT NULL
);

CREATE INDEX idx_fact_sale_date     ON fact_sale(date_sk);
CREATE INDEX idx_fact_sale_customer ON fact_sale(customer_sk);
CREATE INDEX idx_fact_sale_seller   ON fact_sale(seller_sk);
CREATE INDEX idx_fact_sale_product  ON fact_sale(product_sk);
CREATE INDEX idx_fact_sale_store    ON fact_sale(store_sk);
CREATE INDEX idx_fact_sale_supplier ON fact_sale(supplier_sk);