from alpha_vantage.timeseries import TimeSeries
from kafka import KafkaProducer
import time
from json import dumps
from key import api_key

api_key = api_key
print("Kafka Producer Application Started ... ")
kafka_producer_obj = KafkaProducer(bootstrap_servers='localhost:9092',
                   value_serializer=lambda x: dumps(x).encode('utf-8'))
def processData(ticker):
   ts = TimeSeries(key=api_key, output_format='pandas')
   intraData, meta_data=ts.get_intraday(symbol=ticker,interval='5min', outputsize='compact')
   #Remove enumeration from col names
   for column in intraData.columns:
      intraData.rename({column: column.split('. ')[1]}, axis=1, inplace=True)
   return intraData

data = processData('IBM')

for ind in data.index:
    stock = {}
 #   stock['date'] = data['date'][ind]
    stock['date'] = str(ind)
    stock['open'] = data['open'][ind]
    stock['high'] = data['high'][ind]
    stock['low'] = data['low'][ind]
    stock['close'] = data['close'][ind]
    stock['volume'] = data['volume'][ind]
    print("stock to be sent: ", stock)
    kafka_producer_obj.send("capstone3", stock)
    kafka_producer_obj.flush()
    time.sleep(1)
