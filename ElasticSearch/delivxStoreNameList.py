from pymongo import MongoClient
from symspellpy.symspellpy import SymSpell  # import the module
import datetime
import pytz

my_date = datetime.datetime.now(pytz.timezone('Asia/Calcutta'))
my_date = my_date.strftime('%Y-%m-%d %H:%M:%S')

with open('/opt/elasticSearch/delivx_search_python/ElasticSearch/timeJson.txt', 'w') as f:
    f.write("%s\n" % my_date)

client = MongoClient('mongodb://root_DB:cjdk69RvQy5b5VDL@159.203.191.182:5009/DelivX')
db = client.DelivX
store = db.stores.find({},  no_cursor_timeout=True)
sName = []
for i in store:
    if 'sName' in i:
        sName.append(i['sName']['en'])
    else:
        pass

with open('/opt/elasticSearch/delivx_search_python/ElasticSearch/dataJson', 'w') as f:
    for item in sName:
        f.write("%s\n" % item)

max_edit_distance_dictionary = 5
prefix_length = 7
sym_spell = SymSpell(max_edit_distance_dictionary, prefix_length)

if not sym_spell.create_dictionary("/opt/elasticSearch/delivx_search_python/ElasticSearch/dataJson"):
        print("Corpus file not found")

with open('/opt/elasticSearch/delivx_search_python/ElasticSearch/finalStore.txt', 'w') as f:
    for key, count in sym_spell.words.items():
        f.write("%s %s\n" % (key, count))

with open('/opt/elasticSearch/delivx_search_python/ElasticSearch/timeJson.txt', 'w') as f:
    f.write("%s\n" % my_date)
