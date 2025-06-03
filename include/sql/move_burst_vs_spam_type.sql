SELECT
    type,
    sum(case when is_burst then 1 else 0 end) as burst_count,
    sum(case when is_spammable then 1 else 0 end) as spammable_count,
FROM (
    SELECT
        m.type,
        case
            when power >= 120  and pp <= 5 then true
        else false
        end as is_burst,
        case
            when power <= 100 and coalesce(accuracy, 100) = 100 and pp >= 20 then true
        else false
        end as is_spammable,
    FROM
        '{{ silver_path }}/move.parquet' m
) move_grouped
group by type