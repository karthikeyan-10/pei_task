from utils.data_utils import raw_data_to_df, clean_column_names, write_df_to_table
from utils.data_enrich_utils import enrich_customer_table, enrich_product_table, enrich_order_table
from utils.data_aggregate_utils import profit_agg_df
from data_quality import missing_product_id


def main():
    # Read raw data from source and convert to DF
    customer_raw_df = raw_data_to_df("/Volumes/pei_task/external_raw_data/raw_data_files/Customer.xlsx", "excel", sheet_name="Worksheet")
    product_raw_df = raw_data_to_df("/Volumes/pei_task/external_raw_data/raw_data_files/Products.csv", "csv", escape='"')
    order_raw_df = raw_data_to_df("/Volumes/pei_task/external_raw_data/raw_data_files/Orders.json", "json", multiline=True)

    # Clean schema
    customer_df = clean_column_names(customer_raw_df)
    product_df = clean_column_names(product_raw_df)
    order_df = clean_column_names(order_raw_df)

    # Write raw data to Table (Follow 3 level namespace)
    write_df_to_table(customer_df, "pei_task", "bronze_layer", "customers_raw")
    write_df_to_table(product_df, "pei_task", "bronze_layer", "products_raw")
    write_df_to_table(order_df, "pei_task", "bronze_layer", "orders_raw")

    # DQ on Products
    products_dq_df = missing_product_id(product_df, order_df)
    # Write DQ results to Table for further analysis
    write_df_to_table(products_dq_df, "pei_task", "data_quality", "missing_products")

    # Enrich data
    customer_enrich_df = enrich_customer_table(customer_df)
    product_enrich_df = enrich_product_table(product_df)
    order_enrich_df = enrich_order_table(customer_enrich_df, product_enrich_df, order_df)
    # Write enriched data to Table
    write_df_to_table(customer_enrich_df, "pei_task", "silver_layer", "customers_enriched")
    write_df_to_table(product_enrich_df, "pei_task", "silver_layer", "products_enriched")
    write_df_to_table(order_enrich_df, "pei_task", "silver_layer", "orders_enriched")

    # Aggregate table
    agg_df = profit_agg_df(order_enrich_df)

    # Write Aggregate data to table
    write_df_to_table(agg_df, "pei_task", "gold_layer", "agg_sales")



if __name__ == "__main__":
    main()
