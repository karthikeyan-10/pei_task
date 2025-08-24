from pyspark.sql import DataFrame

def products_dq(products_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    unmatched_products = (
        orders_df
        .join(products_df, orders_df.product_id == products_df.product_id, "left_anti")
        .distinct()
    )

    return unmatched_products


if __name__ == "__main__":
    pass
