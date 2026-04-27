from confluent_kafka import Producer

conf = {'bootstrap.servers': 'pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'KK5AE5WBMLIPKT7J',
        'sasl.password': 'cfltAlG85cdkM17dUtfzrE4Ve2lXKVt5ksfW8x9pSnoVP0A+1ZVKjC12GTmfZ8Fw',
        'client.id': 'Ankush Macbook'}


producer = Producer(conf)

account_id = "10001"
transactions_details = '{"transaction_id": 2, "account_id": 10001, "account_holder": "William Anderson", "account_type": "Business Checking", "bank_branch": "Fort Worth", "state": "OH", "account_balance": 34526.15, "transaction_amount": 2603.48, "transaction_type": "Withdrawal", "transaction_date": "2024-01-03", "description": "Online Shopping"}'
producer.produce('npc_transactions', key=account_id, value=transactions_details, callback=lambda err, msg: print(f"Message sent to topic {msg.topic()} partition {msg.partition()} offset {msg.offset()} Message {msg.key().decode('utf-8')}") if not err else print(f"Failed to deliver message: {err}"))
producer.poll(1)
producer.flush()