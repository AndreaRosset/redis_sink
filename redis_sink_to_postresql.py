import redis
import json
import psycopg2

redis_ip = "localhost"
redis_port = 6379

SP500_list = "SP500"
BTC_list= "BTC"

host = "localhost"
database= "btc_sp500_stocks"
user= "postgres"
password= "postgres"

sql = """INSERT INTO btc_sp500_stocks
             VALUES(%f, %f, %f);"""

r = redis.Redis(host=redis_ip, port=redis_port)
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
cur = conn.cursor()

SP500_BTC_db_entry = {}

def db_insertion(json_to_insert):
    
    cur.execute(sql % (json_to_insert["timestamp"], json_to_insert["BTC_value"], json_to_insert["SP500_value"]))
    conn.commit()
    print(json_to_insert["timestamp"])
    

SP500 = [json.loads(r.blpop(SP500_list)[1].decode('utf8'))]
BTC = [json.loads(r.blpop(BTC_list)[1].decode('utf8'))]
    
while True:
    
    last_SP500 = r.blpop(SP500_list)
    last_SP500 = last_SP500[1].decode('utf8')
    last_SP500 = json.loads(last_SP500)
    last_BTC = r.blpop(BTC_list)
    last_BTC = last_BTC[1].decode('utf8')
    last_BTC = json.loads(last_BTC)
    
    if last_BTC["BTC_value"] != BTC[-1]["BTC_value"]:
        
        SP500.append(last_SP500)
        min = 0
        
        for pos, obj in enumerate(SP500):
            if obj["timestamp"] - last_BTC["timestamp"] < SP500[min]["timestamp"] - last_BTC["timestamp"]:
                min = pos
        
        SP500_BTC_db_entry = {
            "timestamp": last_BTC["timestamp"],
            "BTC_value": last_BTC["BTC_value"],
            "SP500_value": SP500[min]["SP500_value"]
        }
        
        db_insertion(SP500_BTC_db_entry)

        SP500 = [last_SP500]
        BTC = [last_BTC]
        
    elif last_SP500["SP500_value"] != SP500[-1]["SP500_value"]:
        
        BTC.append(last_BTC)
        min = 0
        
        for pos, obj in enumerate(BTC):
            if obj["timestamp"] - last_SP500["timestamp"] < BTC[min]["timestamp"] - last_SP500["timestamp"]:
                min = pos
                
        SP500_BTC_db_entry = {
            "timestamp": last_SP500["timestamp"],
            "BTC_value": BTC[min]["BTC_value"],
            "SP500_value": last_SP500["SP500_value"]
        }
        
        db_insertion(SP500_BTC_db_entry)
        
        SP500 = [last_SP500]
        BTC = [last_BTC]
        
    else:
        
        SP500.append(last_SP500)
        BTC.append(last_BTC)