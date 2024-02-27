SELECT      date
            ,COUNT(DISTINCT ACCOUNT) AS active_addresses

FROM        {{ ref('analytics__network_xrp__transactions_parsed') }}

-- UNLModify transactions are records of validators' online status and does not consume fees
WHERE       TRANSACTION_TYPE != 'UNLModify'
            {% if is_incremental() %}
                AND date > (SELECT MAX(date) FROM {{ this }})
            {% endif %}

GROUP BY    date