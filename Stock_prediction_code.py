#Import required packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

#Establish connection to snowflake database
def return_snowflake_conn():

    user_id = Variable.get('snowflake_userid')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')


    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,  # Example: 'xyz12345.us-east-1'
        warehouse='compute_wh',
        database='dev'
    )
    # Create a cursor object
    return conn.cursor()

#Loads stock data into the Snowflake database
@task
def load_data(records, cur):
    target_table = "dev.raw_data.stock_table"
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE OR REPLACE TABLE {target_table} (date datetime primary key,"\
                    "open float, high float, low float, close float, volume float,"\
                    "symbol string);")
        for r in records: # we want records except the first one
            symbol = str(r[0])
            date = r[1]
            open = float(r[2])
            high = float(r[3])
            low = float(r[4])
            close = float(r[5])
            volume = int(r[6])
            # use parameterized INSERT INTO to handle some special characters such as '
            sql = f"INSERT INTO {target_table} (date, open, high, low, close, volume, symbol) VALUES ('{date}',{open},{high},{low},{close},{volume},'{symbol}')"
            cur.execute(sql)
        cur.execute("COMMIT;")
        return True
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

#Retrieve 90 days data and add date field    
@task
def return_last_90d_price(symbol, vantage_api_key):
    """
    - return the last 90 days of the stock prices of symbol as a list of json strings
    """
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
    r = requests.get(url)
    data = r.json()
    results = []   # empyt list for now to hold the 90 days of stock info (open, high, low, close, volume)
    for s in symbol:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={s}&apikey={vantage_api_key}'
        r = requests.get(url)
        data = r.json()
        results_for_this_symbol = []
        for d in data["Time Series (Daily)"]:   # here d is a date: "YYYY-MM-DD"
            results_for_this_symbol.append((s,d,*data["Time Series (Daily)"][d].values()))
        sorted_results = sorted(results_for_this_symbol, key=lambda x: x[0])
        sorted_results = sorted_results[:90] # Last 90 Days always
        # print(sorted_results)
        results = results + sorted_results
    return results

#Trains a forecasting model on stock price data.
@task
def train(cur, train_input_table, train_view, forecast_function_name):
    """
     - Create a view with training related columns
     - Create a model with the view above
    """

    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        # Inspect the accuracy metrics of your model. 
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
        return True
    except Exception as e:
        print(e)
        raise

#Generates predictions based on the trained model.
@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
     - Generate predictions and store the results to a table named forecast_table.
     - Union your predictions with your historical data, then create the final table
    """
    make_prediction_sql = f"""BEGIN
        -- This is the step that creates your predictions.
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store your predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise


with DAG(
    dag_id = 'Stock_Predict_2',
    start_date = datetime(2024,10,11),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule = '30 20 * * *'
) as dag:
    
    cur = return_snowflake_conn()
    target_table = "dev.raw_data.stock_table"
    vantage_api_key = Variable.get("vantage_api_key")
    records = return_last_90d_price(['AAPL','GOOGL'], vantage_api_key)
    
    
    train_input_table = target_table
    train_view = "dev.analytics.stock_data_view"
    forecast_table = "dev.analytics.stock_data_forecast"
    forecast_function_name = "dev.analytics.predict_stock_price"
    final_table = "dev.analytics.final_stock_data"
    
    # Dependency setup
    load_data(records, cur) >> train(cur, train_input_table, train_view, forecast_function_name) >> predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)