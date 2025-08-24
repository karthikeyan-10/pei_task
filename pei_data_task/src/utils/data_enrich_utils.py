from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

def enrich_customer_table(customer_df: DataFrame) -> DataFrame:
    enriched_customer_df = (
        customer_df.dropDuplicates(["customer_id"]).filter(F.col("customer_id").isNotNull() & (F.trim(F.col("customer_id")) != ""))
        .withColumn("phone_split", F.split(F.col("phone"), r"[xX]"))
        .withColumn("phone_main", F.col("phone_split")[0])
        .withColumn(
        "phone_ext", F.when(F.size(F.col("phone_split")) >= 2, F.col("phone_split").getItem(1)).otherwise(None)
        )
        .withColumn("phone_digits", F.regexp_replace(F.col("phone_main"), r"[^0-9]", ""))
        .withColumn(
            "phone",
            F.when((F.length("phone_digits") == 10) | (F.length("phone_digits") == 13), F.col("phone_digits"))
        ).drop("phone_split", "phone_main", "phone_digits")
        .withColumn(
        "customer_name",
        F.regexp_replace(                             # Remove unwanted characters
            F.regexp_replace(                         # Normalize multiple spaces
                F.trim(F.col("Customer_Name")),       # Trim leading/trailing spaces
                r"\s+", " "
            ),
            r"[^a-zA-Z\s']", ""                        # Keep only letters, spaces, and single quote
        )).withColumn(
        "email",
        F.when(
            F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
            F.col("email")
        ).otherwise(None))
        .withColumn(
        "postal_code", F.col("postal_code").cast("string")
    ).withColumn(
        "postal_code",
        F.when(F.length(F.col("postal_code")) == 4, F.lpad(F.col("postal_code"), 5, "0"))  # pad 4-digit
        .when(F.length(F.col("postal_code")) == 5, F.col("postal_code"))                  # keep 5-digit
        .otherwise(None))
    ).select(
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "phone_ext",
        "address",
        "segment",
        "country",
        "city",
        "state",
        "postal_code",
        "region")
    
    return enriched_customer_df

# write_df_to_table(df, "pei_task", "silver_layer", "customers_enriched")

def enrich_product_table(product_df: DataFrame) -> DataFrame:

    window = Window.partitionBy("product_id").orderBy(F.col("price_per_product").desc())

    enriched_product_df = product_df.filter(F.col("product_id").isNotNull() & (F.trim(F.col("product_id")) != "")).withColumn(
        "price_per_product",
        F.when(F.col("price_per_product").cast("double").isNotNull() & (F.col("price_per_product") > 0),
            F.col("price_per_product").cast("double")
        ).otherwise(None)
    ).withColumn("rn", F.row_number().over(window)).filter("rn = 1").drop("rn")

    return enriched_product_df


def enrich_order_table(customers_enriched: DataFrame, products_enriched: DataFrame, orders_df: DataFrame) -> DataFrame:
    
    # Enrich Orders with Customer + Product
    orders_enriched = (
        orders_df
        .join(customers_enriched.select(
            "customer_id",
            F.col("customer_name"),
            "country"
        ), on = "customer_id", how="left")
        
        # join with products
        .join(products_enriched.select(
            "product_id",
            F.col("category"),
            F.col("sub_category")
        ), on = "product_id", how="left")
        
        # Profit rounded
        .withColumn("profit_rounded", F.round(F.col("profit"), 2))
        .withColumn("order_date", F.to_date(F.col("order_date"), "d/M/yyyy"))
        .withColumn("order_year", F.year(F.to_date(F.col('order_date'), 'd/M/yyyy')))
        .withColumn("ship_date", F.to_date(F.col("ship_date"), "d/M/yyyy"))
        .filter(F.col("category").isNotNull() & F.col("sub_category").isNotNull()) # filter and capture the non-matching prod id for DQ
    ).select(
        "order_id",
        "order_date",
        "order_year",
        "ship_date",
        "ship_mode",
        "customer_id",
        "customer_name",
        "country",
        "product_id",
        "category",
        "sub_category",
        "quantity",
        "price",
        "discount",
        "profit_rounded"
    )

    return orders_enriched