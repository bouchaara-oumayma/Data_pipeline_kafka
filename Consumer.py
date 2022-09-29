from pymongo import MongoClient
# connect to MongoDB
myclient = MongoClient("mongodb://localhost:27017/")
mydb = myclient["message_db"]
mycol = mydb["message"]

# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer

# Import sys module
import sys

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name from where the message will recieve
topicName = 'My_Topic'

# Initialize consumer variable
consumer = KafkaConsumer (topicName, group_id ='group1',bootstrap_servers =bootstrap_servers)

# Read and print message from consumer
for msg in consumer: 
    d = {}
    topic_name = msg.topic
    message = msg.value.decode("utf-8")
    
    d = {"topic_name" : topic_name,
     "message" : message}
    
    mycol.insert_one(d)
    
    print("Topic Name=%s"%(topic_name))
    print("Message=%s"%(message))
    print("--> Message sent to MongoDB")
    print("-------------------------------")
# Terminate the script
sys.exit()
