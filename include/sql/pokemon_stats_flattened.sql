SELECT
    p.id as pokemon_id,
    MAX(ps.base_stat) FILTER (WHERE ps.stat_name = 'hp') as hp,
    MAX(ps.base_stat) FILTER (WHERE ps.stat_name = 'attack') as attack,
    MAX(ps.base_stat) FILTER (WHERE ps.stat_name = 'defense') as defense,
    MAX(ps.base_stat) FILTER (WHERE ps.stat_name = 'special_attack') as special_attack,
    MAX(ps.base_stat) FILTER (WHERE ps.stat_name = 'special_defense') as special_defense,
    MAX(ps.base_stat) FILTER (WHERE ps.stat_name = 'speed') as speed
FROM
    '{{ silver_path }}/pokemon_stats.parquet' ps
JOIN
    '{{ silver_path }}/pokemon.parquet' p
ON
    ps.pokemon_id = p.id
GROUP BY
    p.id