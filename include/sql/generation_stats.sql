SELECT
    generation,
    AVG(hp) as avg_hp,
    AVG(attack) as avg_attack,
    AVG(defense) as avg_defense,
    AVG(special_attack) as avg_special_attack,
    AVG(special_defense) as avg_special_defense,
    AVG(speed) as avg_speed,
FROM 
    '{{ silver_path }}/pokemon_master.parquet'
GROUP BY
    generation