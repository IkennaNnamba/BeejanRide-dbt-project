{% macro deduplicate(relation, primary_key, order_by_column) %}

    select *
    from (
        select *,
            row_number() over (
                partition by {{ primary_key }}
                order by {{ order_by_column }} desc
            ) as row_num
        from {{ relation }}
    )
    where row_num = 1

{% endmacro %}