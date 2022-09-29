# Import KafkaProducer from Kafka library
from kafka import KafkaProducer

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name where the message will publish
topicName = 'My_Topic'

# Initialize producer variable
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

while(True): 
    print("enter your message :")
    message_to_send = input()
    # Publish text in defined topic
    producer.send(topicName, bytes(message_to_send, 'utf-8'))

    # Print message
    print("--> Message sent to Consumer")
    print("----------------------------------")
