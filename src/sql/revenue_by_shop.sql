SELECT 
    country, 
    city, 
    shop_id,
    SUM(transaction_total_amount) AS total_revenue
FROM shop_reports.transactions 
WHERE toDate(transaction_time) BETWEEN '{start_date}' AND '{end_date}'
GROUP BY country, city, shop_id
ORDER BY total_revenue DESC