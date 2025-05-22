SELECT
    p.name as pokemon_name,
    m.name as move_name,
    m.power,
    m.accuracy,
    m.pp,
    m.priority,
    JSON_EXTRACT(m.damage_class, '$.name') as damage_class,
    JSON_EXTRACT(m.type, '$.name') as move_type
FROM 
    '{{silver_path}}/pokemon_moves.parquet' pm
JOIN
    '{{silver_path}}/pokemon.parquet' p 
ON
    pm.pokemon_id = p.id
JOIN
    '{{silver_path}}/move.parquet' m
ON
    pm.move_id = m.id