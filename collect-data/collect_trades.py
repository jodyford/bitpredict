import urllib2
import time
import json
from pymongo import MongoClient
import sys

api = 'https://api.bitfinex.com/v1'
symbol = sys.argv[1]
limit = 1000

string connectionString = 
  @"mongodb://coinprices:yRxZPmafptESXYO1mrjrwyCKBURRFVeHxnXpjKGqXoZceffP2FuI2hAvtWUiKIuiKv0R2zqBIani5fkw3TipKw==@coinprices.documents.azure.com:10255/?ssl=true&replicaSet=globaldb";
MongoClientSettings settings = MongoClientSettings.FromUrl(
  new MongoUrl(connectionString)
);
settings.SslSettings = 
  new SslSettings() { EnabledSslProtocols = SslProtocols.Tls12 };
client = new MongoClient(settings);

    
db = client['coinprices']

ltc_trades = db[symbol+'_trades']


def format_trade(trade):
    '''
    Formats trade data
    '''
    if all(key in trade for key in ('tid', 'symbol', 'amount', 'price', 'timestamp')):
        trade['_id'] = trade.pop('tid')
        trade['symbol] = symbol
        trade['amount'] = float(trade['amount'])
        trade['price'] = float(trade['price'])
        trade['timestamp'] = float(trade['timestamp'])

    return trade


def get_json(url):
    '''
    Gets json from the API
    '''
    resp = urllib2.urlopen(url)
    return json.load(resp, object_hook=format_trade), resp.getcode()


print 'Running...'
last_timestamp = 0
while True:
    start = time.time()
    url = '{0}/trades/{1}usd?timestamp={2}&limit_trades={3}'\
        .format(api, symbol, last_timestamp, limit)
    try:
        trades, code = get_json(url)
    except Exception as e:
        print e
        sys.exc_clear()
    else:
        if code != 200:
            print code
        else:
            for trade in trades:
                ltc_trades.update_one({'_id': trade['_id']},
                                      {'$setOnInsert': trade}, upsert=True)
            last_timestamp = trades[0]['timestamp'] - 5
            time_delta = time.time()-start
            if time_delta < 1.0:
                time.sleep(1-time_delta)
