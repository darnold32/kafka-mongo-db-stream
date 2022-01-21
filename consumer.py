from kafka import KafkaConsumer
import pymongo
import json

# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# mydb = myclient["kafka"]
# mycol = mydb["stream"]
# database_names = myclient.list_database_names()
# print ("\ndatabases:", database_names)

consumer = KafkaConsumer('dantopic', bootstrap_servers="127.0.0.1:29092")
for msg in consumer:
    # doc = {
    #     "message" : msg.value
    # }
    #x = mycol.insert_one(doc)
    print(str(msg.value))