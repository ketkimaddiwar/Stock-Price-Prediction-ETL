use role ACCOUNTADMIN;
use warehouse COMPUTE_WH;
use database DEV;
use schema RAW_DATA;


-- Alternate code to setup ML forecasting tasks directly in Snowflake (without using Airflow)


CREATE VIEW STOCK_TABLE_v1 AS SELECT
    to_timestamp_ntz(DATE) as DATE_v1,
    CLOSE,
    SYMBOL
FROM STOCK_TABLE;

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE SNOWFLAKE.ML.FORECAST stock_price(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'STOCK_TABLE_v1'),
    SERIES_COLNAME => 'SYMBOL',
    TIMESTAMP_COLNAME => 'DATE_v1',
    TARGET_COLNAME => 'CLOSE',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Generate predictions and store the results to a table.
BEGIN
    -- This is the step that creates your predictions.
    CALL stock_price!FORECAST(
        FORECASTING_PERIODS => 7,
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE TABLE My_forecasts_2024_09_10 AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View your predictions.
SELECT * FROM My_forecasts_2024_09_10;

-- Union your predictions with your historical data, then view the results in a chart.
SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM STOCK_TABLE
UNION ALL
SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM My_forecasts_2024_09_10;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of your model. 
CALL stock_price!SHOW_EVALUATION_METRICS();

-- Inspect the relative importance of your features, including auto-generated features. 
CALL stock_price!EXPLAIN_FEATURE_IMPORTANCE();

-----------------------------------------------------------
-- AUTOMATION OF FORECASTING
-----------------------------------------------------------
-- Uncaught exception of type 'STATEMENT_ERROR' on line 2 at position 8 : SQL compilation error: Unknown function FORECAST_SUBSCRIPTIONS_MODEL!FORECAST
-- Update your input data with recent data to make your predictions as accurate as possible. 
CREATE OR REPLACE TASK train_task
WAREHOUSE = "COMPUTE_WH"
SCHEDULE = 'USING CRON 0 0 * * 1  America/Los_Angeles' -- Runs at midnight PT, Monday morning.
AS
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST forecast_subscriptions_model(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'STOCK_TABLE_V1'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE_V1',
        TARGET_COLNAME => 'CLOSE'
    );

-- Unknown function FORECAST_SUBSCRIPTIONS_MODEL!FORECAST
-- FORECAST interface: model created without exogenous features. Please use the forecasting_periods argument to generate a forecast.
CREATE OR REPLACE TASK predict_task
WAREHOUSE = "COMPUTE_WH"
SCHEDULE = 'USING CRON 0 1 * * 1 America/Los_Angeles' -- Runs at 1am PT, Monday morning.
AS
    BEGIN
        CALL forecast_subscriptions_model!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval.
            CONFIG_OBJECT => {'prediction_interval': 0.95}
            -- INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'STOCK_TABLE_V1'),
            -- SERIES_COLNAME => 'SYMBOL',
            -- TIMESTAMP_COLNAME => 'DATE_V1'
        );
        LET x := SQLID;



        
        CREATE OR REPLACE TABLE my_forecasts AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END; 

-- Execute your task immediately to confirm it is working. 
EXECUTE TASK train_task;
EXECUTE TASK predict_task;

SHOW TASKS;

-- View your predictions.
SELECT * FROM my_forecasts;