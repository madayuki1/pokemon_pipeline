select
    ability_name,
    count(*) as count
from 
    '{{ silver_path }}/pokemon_abilities_summary.parquet'
group by ability_name
order by count desc
