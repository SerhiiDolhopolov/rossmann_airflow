SELECT 
    category_name,
    product_name,
    SUM(quantity) AS quantity
FROM shop_reports.transactions 
WHERE toDate(transaction_time) BETWEEN '{start_date}' AND '{end_date}'
GROUP BY category_name, product_name
ORDER BY quantity DESC