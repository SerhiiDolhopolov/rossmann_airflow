SELECT 
    payment_method,
    SUM(transaction_total_amount) AS total_revenue
FROM shop_reports.transactions 
WHERE toDate(transaction_time) BETWEEN '{start_date}' AND '{end_date}'
GROUP BY payment_method
ORDER BY total_revenue DESC