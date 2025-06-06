SELECT
    generation,
    rarity,
    AVG(capture_rate) as avg_capture_rate
FROM
    '{{ silver_path }}/pokemon_master.parquet'
where rarity='legendary' or rarity='normal'
GROUP BY generation, rarity
ORDER BY AVG(capture_rate) DESC