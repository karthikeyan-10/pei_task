-- Profit by Year
SELECT 
    order_year,
    ROUND(SUM(profit_rounded), 2) AS total_profit
FROM pei_task.silver_layer.orders_enriched
GROUP BY order_year
ORDER BY order_year desc;

-- Profit by Year and Category 
SELECT 
    order_year,
    category,
    ROUND(SUM(profit_rounded), 2) AS total_profit
FROM pei_task.silver_layer.orders_enriched
GROUP BY order_year, Category
ORDER BY order_year, category;

-- Profit by Customer
SELECT 
    customer_id,
    customer_name,
    ROUND(SUM(profit_rounded), 2) AS total_profit
FROM pei_task.silver_layer.orders_enriched
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC;

-- Profit by Customer and Year
SELECT 
    order_year,
    customer_id,
    customer_name,
    ROUND(SUM(profit_rounded), 2) AS total_profit
FROM pei_task.silver_layer.orders_enriched
GROUP BY order_year, customer_id, customer_name
ORDER BY order_year desc, total_profit DESC;
