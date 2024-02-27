SELECT      MIN(date) AS date
            ,ACCOUNT AS account

FROM        {{ ref('analytics__network_xrp__transactions_parsed') }}

WHERE       1=1
            {% if is_incremental() %}
                AND date > (SELECT MAX(date) FROM {{ this }})
                AND account NOT IN (SELECT account FROM {{ this }})
            {% endif %}

GROUP BY    2