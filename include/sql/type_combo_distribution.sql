SELECT
    type_combo,
    COUNT(*) AS count
FROM 
    '{{ silver_path }}/type_combo.parquet'
GROUP BY type_combo
ORDER BY count DESC;