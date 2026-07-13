-- Staging: read raw trades from the S3 data lake and clean them up
-- Source: newline-delimited JSON files written by our Lambda function

with raw_trades as (

    select *
    from read_json_auto(
        's3://crypto-trades-lake-meghana-2026/raw/**/*.json',
        format = 'newline_delimited'
    )

)

select
    cast(trade_id as bigint)          as trade_id,
    cast(product as varchar)          as product,
    cast(side as varchar)             as side,
    cast(price as double)             as price,      -- was text, now numeric
    cast(size as double)              as size,       -- was text, now numeric
    cast(timestamp as timestamp)      as traded_at,
    cast(price as double) * cast(size as double) as trade_value   -- derived: $ value of the trade

from raw_trades
where trade_id is not null
  and price is not null
  and size is not null