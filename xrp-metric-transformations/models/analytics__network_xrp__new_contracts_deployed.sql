SELECT      date
            ,COUNT(hash) AS new_contracts_deployed

FROM        {{ ref('analytics__network_xrp__transactions_parsed') }}

WHERE       TRANSACTION_TYPE = 'EscrowCreate'
            AND TRANSACTION_RESULT = 'tesSUCCESS'

            {% if is_incremental() %}
                AND date > (SELECT MAX(date) FROM {{ this }})
            {% endif %}

GROUP BY    date