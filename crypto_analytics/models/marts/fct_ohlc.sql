-- Mart: OHLC candles — the standard way to summarize price over time windows
-- One row per product, per minute

with trades as (

    select * from {{ ref('stg_trades') }}

),

candles as (

    select
        product,
        date_trunc('minute', traded_at) as candle_minute,

        -- OHLC: the four classic price points in each window
        first(price order by traded_at)  as open_price,
        max(price)                       as high_price,
        min(price)                       as low_price,
        last(price order by traded_at)   as close_price,

        -- Volume metrics
        sum(size)                        as volume,
        sum(trade_value)                 as traded_value_usd,
        count(*)                         as trade_count

    from trades
    group by product, date_trunc('minute', traded_at)

)

select
    product,
    candle_minute,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    traded_value_usd,
    trade_count,
    -- Derived: how much price moved in this minute
    close_price - open_price                                as price_change,
    round(((close_price - open_price) / open_price) * 100, 4) as price_change_pct

from candles
order by product, candle_minute