SELECT      date,
            COUNT(account) AS new_addresses

FROM        {{ ref('analytics__network_xrp__new_account_lookups') }}

WHERE       1=1
            {% if is_incremental() %}
                AND date > (SELECT MAX(date) FROM {{ this }})
            {% endif %}

GROUP BY    date