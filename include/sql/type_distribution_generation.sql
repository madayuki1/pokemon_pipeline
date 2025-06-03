SELECT
    pm.generation,
    tc.type_1,
    tc.type_2,
FROM
    '{{ silver_path }}/pokemon_master.parquet' pm
JOIN '{{ silver_path }}/type_combo.parquet' tc ON pm.pokemon_id = tc.pokemon_id