import urllib2
import time
import json
from pymongo import MongoClient
import sys

api = 'https://api.bitfinex.com/v1'
symbol = sys.argv[1]
limit = 25
book_url = '{0}/book/{1}usd?limit_bids={2}&limit_asks={2}'\
    .format(api, symbol, limit)

string connectionString = 
  @"mongodb://coinprices:yRxZPmafptESXYO1mrjrwyCKBURRFVeHxnXpjKGqXoZceffP2FuI2hAvtWUiKIuiKv0R2zqBIani5fkw3TipKw==@coinprices.documents.azure.com:10255/?ssl=true&replicaSet=globaldb";
MongoClientSettings settings = MongoClientSettings.FromUrl(
  new MongoUrl(connectionString)
);
settings.SslSettings = 
  new SslSettings() { EnabledSslProtocols = SslProtocols.Tls12 };
client = new MongoClient(settings);

    
db = client['coinprices']
ltc_books = db[symbol+'_books']


def format_book_entry(entry):
    '''
    Converts book data to float
    '''
    if all(key in entry for key in ('symbol','amount', 'price', 'timestamp')):
        entry['symbol'] = symbol
        entry['amount'] = float(entry['amount'])
        entry['price'] = float(entry['price'])
        entry['timestamp'] = float(entry['timestamp'])
    return entry


def get_json(url):
    '''
    Gets json from the API
    '''
    resp = urllib2.urlopen(url)
    return json.load(resp, object_hook=format_book_entry), resp.getcode()


print 'Running...'
while True:
    start = time.time()
    try:
        book, code = get_json(book_url)
    except Exception as e:
        print e
        sys.exc_clear()
    else:
        if code != 200:
            print code
        else:
            book['_id'] = time.time()
            ltc_books.insert_one(book)
            time_delta = time.time()-start
            if time_delta < 1.0:
                time.sleep(1-time_delta)
