INSERT INTO FUNCTION s3(
        '{s3_file_path}',
        '{s3_access_key}',
        '{s3_secret_key}',
        'Parquet'
    )
    SETTINGS s3_truncate_on_insert=1
    SELECT * FROM shop_reports.delivery 
    WHERE toDate(accepted_time) BETWEEN '{month_start}' AND '{next_month_start}';