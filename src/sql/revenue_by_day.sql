SELECT 
    country, 
    city,  
    shop_id,
    toDate(transaction_time) AS day,
    SUM(transaction_total_amount) AS total_revenue
FROM shop_reports.transactions 
WHERE toDate(transaction_time) BETWEEN '{start_date}' AND '{end_date}'
GROUP BY country, city, shop_id, day
ORDER BY day, total_revenue DESC