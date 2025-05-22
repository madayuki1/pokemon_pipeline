SELECT 
    p.name as pokemon_name,
    a.name as ability_name,
    a.short_effect_en
FROM 
    '{{silver_path}}/pokemon_abilities.parquet' pa
JOIN
    '{{silver_path}}/pokemon.parquet' p
ON
    pa.pokemon_id = p.id
JOIN
    '{{silver_path}}/ability.parquet' a
ON
    pa.ability_id = a.id