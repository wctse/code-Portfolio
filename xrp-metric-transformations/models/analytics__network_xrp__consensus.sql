-- Most data are null because the XRP ledger history lacks consensus data

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

SELECT      dates.date
            ,'XRP' AS network
            ,'xrp' AS slug
            ,NULL AS total_validators
            ,NULL AS total_block_producers
            ,NULL AS total_stake
            ,NULL AS stake_token
            ,NULL AS estimated_nakamoto_coefficient

FROM        dates

ORDER BY    1