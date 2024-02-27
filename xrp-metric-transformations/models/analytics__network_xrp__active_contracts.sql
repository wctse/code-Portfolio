-- Calculate the amount of active contracts (escrows) by finding the difference of created and cancelled/finished escrows each day
-- The lost data before 2023-01-01 would not affect the accuracy of the active contracts metric because escrows were introduced in 2017
{% set transactions = ref('analytics__network_xrp__transactions_parsed') %}

WITH created_escrows AS (
    SELECT      date
                ,COUNT(hash) AS count_created

    FROM        {{ transactions }}

    WHERE       TRANSACTION_TYPE = 'EscrowCreate'
                AND TRANSACTION_RESULT = 'tesSUCCESS'

                {% if is_incremental() %}
                    AND date > (SELECT MAX(date) FROM {{ this }})
                {% endif %}

    GROUP BY    date
),

cancelled_or_finished_escrows AS (
    SELECT      date
                ,COUNT(hash) AS count_cancelled_or_finished

    FROM        {{ transactions }}

    WHERE       (
                    TRANSACTION_TYPE = 'EscrowCancel'
                    OR TRANSACTION_TYPE = 'EscrowFinish'
                )
                AND TRANSACTION_RESULT = 'tesSUCCESS'

                {% if is_incremental() %}
                    AND date > (SELECT MAX(date) FROM {{ this }})
                {% endif %}
    
    GROUP BY    date
),

-- The net amount of created escrows minus cancelled or finished escrows each day
daily_change AS (
    SELECT      date,
                SUM(count_created) - SUM(count_cancelled_or_finished) AS daily_change
    
    FROM        created_escrows
                INNER JOIN cancelled_or_finished_escrows USING (date)

    WHERE       1=1
                {% if is_incremental() %}
                    AND date > (SELECT MAX(date) FROM {{ this }})
                {% endif %}

    GROUP BY    date
)

{% if is_incremental() %}
-- For incremental builds, just add the last date's value and the latest daily change
SELECT      dc.date AS date
            ,daily_change + {{ this }}.active_contracts AS active_contracts

FROM        daily_change dc
            INNER JOIN {{ this }} ON dc.date = DATE_ADD({{ this }}.date, INTERVAL 1 DAY)

WHERE       dc.date = DATE_ADD((SELECT MAX(date) FROM {{ this }}), INTERVAL 1 DAY)

{% else %}
-- For non-incremental builds, sum the net daily change until the specific date to get the daily active contracts
SELECT      date
            ,SUM(daily_change) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS active_contracts

FROM        daily_change

WHERE       1=1

{% endif %}