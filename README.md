# CanadianTire Promo Analysis

## Process
### - Loading
extract the dataset from .csv file to PostgreSQL Database

### - Data Manipulation    

ID column is wrongly exported as NUMERIC columns, hence missing leading 0s for some values. Originally it is a STRING with length 4
Cannibalization effects analysis

### -    Data Aggregation     
The original ‘sales’ data is in DATE level, and needs to be aggregated into YEAR-WEEK level.

### -       Data Calculation
Estimated regular sales for each promotion week.

### - Peak/off season analysis for products consecutively promoted for more than 3 week

### - Lift the ESTIMATED_REG_UNITS which is underestimated as the REG_UNITS used to calculate it are in off-peak seasons
#### A rule-based lifting method is introduced:
Lift the sum of ESTIMATED_REG_UNITS in the promotion window (length>=3) by a calculated degree “UP_DEGREE”, which is sum of REG_UNITS divided by sum of ESTIMATED_REG_UNITS of both window frames, regardless of whether it’s ‘R’ or ‘D’.

If “UP_DEGREE” is < 0, then convert it to 1.
Redistribute sum of lifted ESTIMATED_REG_UNITS according to the weights of PROMO_UNITS, which is each PROMO_UNITS divided by sum of PROMO_UNITS in promotion window, named “PROMO_WEIGHT”.

For example, presuming a promotion window with size 3 has PROMO_UNITS [a, b, c], the “PROMO_WEIGHT” of each promotion week is   
      [a/(a+b+c), b/(a+b+c), c/(a+b+c)]
