#!/usr/bin/env python3

import clickhouse_connect

client = clickhouse_connect.get_client(
    host="lab2_clickhouse",
    port=8123,
    username="default",
    password=""
)

queries = [
    ("mart_top_products", "SELECT * FROM mart_top_products LIMIT 10"),
    ("mart_category_revenue", "SELECT * FROM mart_category_revenue LIMIT 10"),
    ("mart_product_rating_reviews", "SELECT * FROM mart_product_rating_reviews LIMIT 10"),
    ("mart_top_customers", "SELECT * FROM mart_top_customers LIMIT 10"),
    ("mart_customer_country_dist", "SELECT * FROM mart_customer_country_dist LIMIT 10"),
    ("mart_customer_avg_check", "SELECT * FROM mart_customer_avg_check LIMIT 10"),
    ("mart_monthly_trend", "SELECT * FROM mart_monthly_trend LIMIT 10"),
    ("mart_yearly_revenue", "SELECT * FROM mart_yearly_revenue LIMIT 10"),
    ("mart_yearly_compare", "SELECT * FROM mart_yearly_compare LIMIT 10"),
    ("mart_avg_order_month", "SELECT * FROM mart_avg_order_month LIMIT 10"),
    ("mart_top_stores", "SELECT * FROM mart_top_stores LIMIT 10"),
    ("mart_store_city_country", "SELECT * FROM mart_store_city_country LIMIT 10"),
    ("mart_store_avg_check", "SELECT * FROM mart_store_avg_check LIMIT 10"),
    ("mart_top_suppliers", "SELECT * FROM mart_top_suppliers LIMIT 10"),
    ("mart_supplier_avg_price", "SELECT * FROM mart_supplier_avg_price LIMIT 10"),
    ("mart_supplier_country_dist", "SELECT * FROM mart_supplier_country_dist LIMIT 10"),
    ("mart_highest_rated", "SELECT * FROM mart_highest_rated LIMIT 10"),
    ("mart_lowest_rated", "SELECT * FROM mart_lowest_rated LIMIT 10"),
    ("mart_most_reviewed", "SELECT * FROM mart_most_reviewed LIMIT 10"),
    ("mart_rating_sales_correlation", "SELECT * FROM mart_rating_sales_correlation LIMIT 1"),
]

for name, q in queries:
    print(f"\n=== {name} ===")
    try:
        res = client.query(q)
        for row in res.result_rows:
            print(row)
    except Exception as e:
        print(f"Ошибка: {e}")