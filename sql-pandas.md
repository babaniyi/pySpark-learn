# Row Number within each group (partition)

> SQL
- ROW_NUMBER() over (PARTITION BY ticker ORDER BY date DESC) as days_lookback

- ROW_NUMBER() over (PARTITION BY ticker, exchange ORDER BY date DESC, period) as example_2

> PANDAS
- df['days_lookback'] = df.sort_values(['date'], ascending=False)\
             .groupby(['ticker'])\
             .cumcount() + 1
             
- df['example_2'] = df.sort_values(['date', 'period'], \
             ascending=[False, True])\
             .groupby(['ticker', 'exchange'])\
             .cumcount() + 1

# Last/Next record within each group (partition)
- LAG(price) over (PARTITION BY ticker ORDER BY date) as last_day_px
- LEAD(price) over (PARTITION BY ticker ORDER BY date) as next_day_px

> PANDAS
- df['last_day_px'] = df.sort_values(by=['date'], ascending=True)\
                       .groupby(['ticker'])['price'].shift(1)

- df['next_day_px'] = df.sort_values(by=['date'], ascending=True)\
                       .groupby(['ticker'])['price'].shift(-1)

# Percentile rank within each group
- PERCENT_RANK() OVER (PARTITION BY ticker, year ORDER BY price) as perc_price

- df['perc_price'] = df.groupby(['ticker', 'year'])['price'].rank(pct=True)

# Running Sum within each group
- SUM(trade_vol) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 3 PRECEEDING AND CURRENT ROW) as volume_3day
- SUM(trade_vol) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN UNBOUNDED PRECEEDING AND CURRENT ROW) as cum_total_vol
- SUM(trade_vol) OVER (PARTITION BY ticker) as total_vol

> PANDAS
- df['volume_3day'] = df.sort_values(by=['date'], ascending=True).groupby(['ticker'])['trade_vol'].rolling(3, min_periods = 1).sum().reset_index(drop=True, level=0)

- df['cum_total_vol'] = df.sort_values(by=['date'], ascending=True).groupby(['ticker'])['trade_vol'].cumsum()

- df['total_vol'] = df.groupby(['ticker', 'year'])['trade_vol'].transform('sum')

  # Average within each group
- AVG(trade_vol) OVER (PARTITION BY ticker) as avg_trade_vol,
- AVG(price) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma2

> PANDAS
- df['avg_trade_vol'] = df.groupby(['ticker', 'year'])['trade_vol'].transform('mean')

- df['ma20'] = df.sort_values(by=['date'], ascending=True).groupby('ticker')['price'].rolling(20, min_periods = 1).mean().reset_index(drop=True, level=0)
