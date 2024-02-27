SELECT      date
            ,COUNT(HASH) AS successful_transactions

FROM        {{ ref('analytics__network_xrp__transactions_parsed') }}

WHERE       TRANSACTION_RESULT = 'tesSUCCESS'
            {% if is_incremental() %}
                AND date > (SELECT MAX(date) FROM {{ this }})
            {% endif %}

GROUP BY    date