from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def profit_agg_df(orders_enriched_df: DataFrame) -> DataFrame:
    profit_agg_df = (
        orders_enriched_df.withColumn("Year", F.year("order_date"))
        .groupBy("Year", "category", "sub_category", "customer_id")
        .agg(
            F.round(F.sum("profit_rounded"), 2).alias("total_profit"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("total_quantity"),
            F.round(F.sum("price"), 2).alias("total_sales"),
        )
    )

    return profit_agg_df

if __name__ == "__main__":
    pass