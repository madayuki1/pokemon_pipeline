SELECT
    p.name as pokemon_name,
    m.name as move_name,
    m.power,
    m.accuracy,
    m.pp,
    m.priority,
    m.damage_class,
    m.type,
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