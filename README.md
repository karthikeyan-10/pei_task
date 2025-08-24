PEI Data Engineering Task.

The main intent of this task is to build an aggregate table that shows profit by various categories. To achieve the final aggregate table, there are various intermediate data analysis, cleaning and transformations are applied. 

The entire implementation is done using Pyspark, Pandas and SQL. **Poetry** is used as dependency management tool. 

The source files include the sample US sales data from three different files.
> Customer.xlsx,
> Products.csv,
> Orders.json

Assumptions made:
> The data in the Products table have duplicate product_id and among them unique rows are filtered based on maximum price_per_product.

- Task 1:
The raw data is initially read and stored in the 'bronze_layer' schema.

- Task 2 and 3:
The enriched data is stored in the 'silver_layer' schema.

- Task 4:
Finally the aggregated data is stored in the 'gold_layer' schema.

The tables are stored in the 3 layer namespace to align with Unity Catalog. 
The data quality is done on Products and Orders table and missing products are stored in 'data_quality' schema. 

- Task 5:
The SQL output on aggregated table is attached below.
> Profit by Year
<img width="797" height="504" alt="image" src="https://github.com/user-attachments/assets/13d07a84-edb8-4224-a454-6fc3386ea853" />

> Profit by year and Category
<img width="979" height="756" alt="image" src="https://github.com/user-attachments/assets/726bef2a-bf16-4416-ad3e-6896c04c324c" />

> Profit by Customer
<img width="820" height="752" alt="image" src="https://github.com/user-attachments/assets/d57c16fb-344f-4a17-8e14-d736fa64d4e3" />

> Profit by Customer and Year
<img width="832" height="762" alt="image" src="https://github.com/user-attachments/assets/3414b7d0-519a-49ab-ac15-d21d621cdb92" />


Scope for enhancement:
- The entire flow can be driven with the yaml config file and can have various options to include databricks features like **liquid clustering**, delta table configs, vaccum, optimize, DLT, etc..
- Add more DQ to check primary_key uniqueness and staleness data.
- Schedule the job to run on certain interval using Databricks jobs and workflow.
- Build dashboard on final master table.

