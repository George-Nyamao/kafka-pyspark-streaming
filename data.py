import time
import pandas as pd
from datetime import datetime
from alpha_vantage.timeseries import TimeSeries
from json import dumps
import json
import matplotlib.pyplot as plt
from pip import __main__

ts = TimeSeries(key='FO2TBZIS1BRKDHTO', output_format='pandas')
all_data = ts.get_intraday(symbol='MSFT',interval='1min', outputsize='compact')
df = all_data[0]
for col in df.columns:
        df.rename({col: col.split('. ')[1]}, axis=1,inplace=True)
print(df)
def get_data():
    return df
