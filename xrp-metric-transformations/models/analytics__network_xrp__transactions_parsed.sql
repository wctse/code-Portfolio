-- The CLOSE_TIME_HUMAN column is the only timestamp available in the XRP_TRANSACTIONS table, but it is in a format very unfriendly to SQL
-- Here we preprocess the timestamp and select the columns we need

{% set transactions = source('NETWORK_METRICS', 'XRP_TRANSACTIONS') %}

SELECT      DATE_TRUNC('day', TO_TIMESTAMP(CLOSE_TIME_HUMAN, 'YYYY-Mon-DD HH24:MI:SS.FF UTC')) AS DATE
            ,HASH
            ,ACCOUNT
            ,PARSE_JSON(META_DATA):TransactionResult::string AS TRANSACTION_RESULT
            ,TRANSACTION_TYPE
            ,FEE

FROM        {{ transactions }}

WHERE       1=1
            {% if is_incremental() %}
                AND DATE_TRUNC('day', TO_TIMESTAMP(CLOSE_TIME_HUMAN, 'YYYY-Mon-DD HH24:MI:SS.FF UTC')) > (SELECT MAX(date) FROM {{ this }})
            {% endif %}