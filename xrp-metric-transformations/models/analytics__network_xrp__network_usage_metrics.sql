WITH dates AS (
    WITH generate_dates AS (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('" ~ var('arweave-start-date') ~ "' as date)",
            end_date="DATEADD(DAY, 1, CURRENT_DATE())"
        )
        }}
    )
    SELECT DATE_DAY AS date FROM generate_dates
)

SELECT      date
            ,'XRP' AS network
            ,'xrp' AS slug
            ,COALESCE(aa.active_addresses, 0) AS active_addresses
            ,COALESCE(na.new_addresses, 0) AS new_addresses
            ,COALESCE(aa.active_addresses - na.new_addresses, 0) AS returning_addresses
            ,COALESCE(tt.total_transactions, 0) AS total_transactions
            ,COALESCE(st.successful_transactions, 0) AS successful_transactions
            ,COALESCE(tt.total_transactions - st.successful_transactions, 0) AS unsuccessful_transactions
            ,COALESCE(tt.transaction_fees, 0) AS transaction_fees
            ,'XRP' AS transaction_fee_token
            ,COALESCE(ac.active_contracts, 0) AS active_contracts
            ,NULL AS total_contract_calls
            ,NULL AS new_contracts_called
            ,NULL AS returning_contracts_called
            ,NULL AS unique_contract_callers
            ,COALESCE(ncd.new_contracts_deployed, 0) AS new_contracts_deployed

FROM        dates
            LEFT JOIN {{ ref('analytics__network_xrp__active_addresses') }} aa USING (date)
            LEFT JOIN {{ ref('analytics__network_xrp__new_addresses') }} na USING (date)
            LEFT JOIN {{ ref('analytics__network_xrp__total_transactions') }} tt USING (date)
            LEFT JOIN {{ ref('analytics__network_xrp__successful_transactions') }} st USING (date)
            LEFT JOIN {{ ref('analytics__network_xrp__active_contracts') }} ac USING (date)
            LEFT JOIN {{ ref('analytics__network_xrp__new_contracts_deployed') }} ncd USING (date)

ORDER BY    date