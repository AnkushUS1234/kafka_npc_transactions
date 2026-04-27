from confluent_kafka import Producer
import json

conf = {'bootstrap.servers': 'pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'user',
        'sasl.password': 'pwd',
        'client.id': 'Ankush Macbook'}


producer = Producer(conf)

with open('banking_input.json', 'r') as file:
    for line in file:
        order = json.loads(line)
        account_id = str(order["account_id"])
        transactions_details = line
        
        producer.produce('npc_transactions', key=account_id, value=transactions_details, callback=lambda err, msg: print(f"Message sent to topic {msg.topic()} partition {msg.partition()} offset {msg.offset()} Message {msg.key().decode('utf-8')}") if not err else print(f"Failed to deliver message: {err}"))

producer.poll(1)
producer.flush()