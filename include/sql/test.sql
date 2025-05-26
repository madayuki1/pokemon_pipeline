SELECT
    CASE
        WHEN t1.name < t2.name THEN t1.name || '/' || COALESCE(t2.name, 'None')
        WHEN t2.name is NULL THEN t1.name || '/None'
        ELSE t2.name || '/' || t1.name
    END AS type_combo,
    COUNT(*) as count
FROM (
    SELECT
        pt.pokemon_id,
        t.name AS type_name,
        ROW_NUMBER() OVER (PARTITION BY pt.pokemon_id ORDER BY t.name) AS rn
    FROM "{{silver_path}}/pokemon_types.parquet" AS pt
    JOIN "{{silver_path}}/type.parquet" AS t ON pt.type_id = t.id
) AS pt_ranked
JOIN "{{silver_path}}/pokemon.parquet" AS p ON pt_ranked.pokemon_id = p.id
LEFT JOIN (
    SELECT
        pt.pokemon_id,
        t.name AS type_name,
        ROW_NUMBER() OVER (PARTITION BY pt.pokemon_id ORDER BY t.name) AS rn
    FROM "{{silver_path}}/pokemon_types.parquet" AS pt
    JOIN "{{silver_path}}/type.parquet" AS t ON pt.type_id = t.id
) AS pt2 ON pt_ranked.pokemon_id = pt2.pokemon_id AND pt2.rn = 2
JOIN "{{silver_path}}/type.parquet" AS t1 ON pt_ranked.type_name = t1.name
LEFT JOIN "{{silver_path}}/type.parquet" AS t2 ON pt2.type_name = t2.name
GROUP BY
    type_combo
ORDER BY
    count DESC