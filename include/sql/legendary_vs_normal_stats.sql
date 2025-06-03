SELECT
    pm.pokemon_id,
    pm.pokemon_name,
    pm.is_legendary,
    pm.is_mythical,
    pm.hp,
    pm.attack,
    pm.defense,
    pm.special_attack,
    pm.special_defense,
    pm.speed,
    pm.hp + pm.attack + pm.defense +
    pm.special_attack + pm.special_defense + pm.speed AS total_stats
FROM
    '{{ silver_path }}/pokemon_master.parquet' pm
{# WHERE pokemon_name='garchomp' #}
WHERE is_mythical='false'
ORDER BY total_stats DESC