SELECT
    generation,
    AVG(capture_rate)
FROM
    '{{ silver_path }}/pokemon_master.parquet'
WHERE is_mythical!='true'
GROUP BY generation
ORDER BY AVG(capture_rate) DESC