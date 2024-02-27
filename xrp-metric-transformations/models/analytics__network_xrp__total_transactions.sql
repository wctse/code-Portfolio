SELECT      date
            ,COUNT(HASH) AS total_transactions
            ,SUM(FEE / 1e6) AS transaction_fees

FROM        {{ ref('analytics__network_xrp__transactions_parsed') }}

WHERE       1=1
            {% if is_incremental() %}
                AND date > (SELECT MAX(date) FROM {{ this }})
            {% endif %}

GROUP BY    date