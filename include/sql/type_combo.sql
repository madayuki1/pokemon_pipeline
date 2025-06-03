SELECT
    pokemon_id,
    type_1,
    type_2,
    CASE
        WHEN type_2 IS NULL THEN type_1 || '/None'
        WHEN type_1 < type_2 THEN type_1 || '/' || type_2
        ELSE type_2 || '/' || type_1
    END AS type_combo,
FROM (
    SELECT
        pt.pokemon_id,
        MAX(CASE WHEN pt.slot = 1 THEN t.name END) AS type_1,
        MAX(CASE WHEN pt.slot = 2 THEN t.name END) AS type_2
    FROM "{{silver_path}}/pokemon_types.parquet" pt
    JOIN "{{silver_path}}/type.parquet" t ON pt.type_id = t.id
    GROUP BY pt.pokemon_id
) typed