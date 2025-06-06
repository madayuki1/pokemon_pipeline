SELECT
    p.id as pokemon_id,
    p.name as pokemon_name,
    ps.generation,
    tc.type_combo,
    p.height,
    p.weight,
    ps.capture_rate,
    psf.hp,
    psf.attack,
    psf.defense,
    psf.special_attack,
    psf.special_defense, 
    psf.speed,
    case
        when is_legendary then 'legendary'
        when is_mythical then 'mythical'
        else 'normal'
    end as rarity
FROM
    '{{ silver_path }}/pokemon.parquet' p 
JOIN '{{ silver_path }}/pokemon_species.parquet' ps ON p.species = ps.id
JOIN '{{ silver_path }}/type_combo.parquet' tc ON p.id = tc.pokemon_id
JOIN '{{ silver_path }}/pokemon_stats_flattened.parquet' psf ON p.id = psf.pokemon_id