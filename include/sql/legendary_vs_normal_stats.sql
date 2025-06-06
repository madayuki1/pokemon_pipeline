SELECT
    pm.pokemon_id,
    pm.pokemon_name,
    pm.rarity,
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
where rarity!='mythical'
ORDER BY total_stats DESC