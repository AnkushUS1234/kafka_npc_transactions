#!/usr/bin/env python3
"""
Kafka Consumer for Banking Transactions - Writes to MongoDB
Reads from Kafka topic and inserts documents into MongoDB
"""

from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== KAFKA CONFIGURATION ====================
KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'user',
    'sasl.password': 'pwd',
    'group.id': 'banking-mongodb-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

# ==================== MONGODB CONFIGURATION ====================
# Replace with your MongoDB connection string
MONGODB_URI = 'mongodb+srv://ankush_db_user:WHBr5CO4JJ7cYmw2@clusternpcbank.lnwuht2.mongodb.net/'
MONGODB_DATABASE = 'banking_db'
MONGODB_COLLECTION = 'transactions'

# SSL certificate handling for MongoDB Atlas
import ssl
MONGODB_CONNECTION_OPTIONS = {
    'tls': True,
    'tlsAllowInvalidCertificates': True  # For development/testing only
}

# ==================== KAFKA TOPIC ====================
KAFKA_TOPIC = 'npc_transactions'  # Change if using different topic for banking data


class MongoDBConsumer:
    """Kafka Consumer that writes transactions to MongoDB"""
    
    def __init__(self):
        """Initialize Kafka Consumer and MongoDB connection"""
        try:
            # Initialize Kafka Consumer
            self.kafka_consumer = Consumer(KAFKA_CONFIG)
            self.kafka_consumer.subscribe([KAFKA_TOPIC])
            logger.info(f"✓ Connected to Kafka topic: {KAFKA_TOPIC}")
            
            # Initialize MongoDB connection with SSL options
            self.mongo_client = MongoClient(MONGODB_URI, **MONGODB_CONNECTION_OPTIONS)
            self.db = self.mongo_client[MONGODB_DATABASE]
            self.collection = self.db[MONGODB_COLLECTION]
            
            # Create index on transaction_id for uniqueness
            self.collection.create_index('transaction_id', unique=True)
            
            logger.info(f"✓ Connected to MongoDB: {MONGODB_DATABASE}.{MONGODB_COLLECTION}")
            
        except Exception as e:
            logger.error(f"✗ Connection failed: {e}")
            raise
    
    def process_message(self, message_value):
        """
        Parse and validate message
        
        Args:
            message_value: JSON string from Kafka
            
        Returns:
            dict: Parsed message or None if invalid
        """
        try:
            data = json.loads(message_value)
            # Add timestamp when inserted into MongoDB
            data['inserted_at'] = datetime.utcnow()
            return data
        except json.JSONDecodeError as e:
            logger.error(f"✗ Invalid JSON: {e}")
            return None
    
    def insert_into_mongodb(self, document):
        """
        Insert document into MongoDB
        
        Args:
            document: Dictionary to insert
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self.collection.insert_one(document)
            return True
        except Exception as e:
            logger.error(f"✗ MongoDB insert failed: {e}")
            return False
    
    def consume_messages(self, timeout_ms=5000):
        """
        Polls Kafka repeatedly. As long as messages are received, keeps polling immediately.
        If no message is received for 5 seconds, exits.
        """
        logger.info(f"Polling Kafka for messages. Will exit if no new message in {timeout_ms/1000} seconds.")
        messages_consumed = 0
        messages_failed = 0
        try:
            while True:
                msg = self.kafka_consumer.poll(timeout_ms / 1000)  # poll expects seconds if float
                if msg is None:
                    logger.info(f"No message received from Kafka in {timeout_ms/1000} seconds. Exiting.")
                    break
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of partition (no new messages). Waiting for more...")
                        continue
                    else:
                        logger.error(f"✗ Kafka error: {msg.error()}")
                        messages_failed += 1
                else:
                    try:
                        key = msg.key().decode('utf-8') if msg.key() else None
                        value = msg.value().decode('utf-8')
                        logger.info(f"Received message with key: {key}")
                        document = self.process_message(value)
                        if document:
                            if self.insert_into_mongodb(document):
                                messages_consumed += 1
                                logger.info(f"📊 Processed {messages_consumed} message(s)")
                            else:
                                messages_failed += 1
                        else:
                            messages_failed += 1
                    except Exception as e:
                        logger.error(f"✗ Error processing message: {e}")
                        messages_failed += 1
        except KeyboardInterrupt:
            logger.info("\n⏹️  Stopping consumer...")
        except Exception as e:
            logger.error(f"✗ Unexpected error during poll: {e}")
        finally:
            logger.info(f"\n📈 Final Statistics:")
            logger.info(f"   Messages Consumed: {messages_consumed}")
            logger.info(f"   Messages Failed: {messages_failed}")
            self.close()
    
    def close(self):
        """Close connections"""
        self.kafka_consumer.close()
        self.mongo_client.close()
        logger.info("✓ Connections closed")


def main():
    """Main function"""
    try:
        consumer = MongoDBConsumer()
        consumer.consume_messages()
    except Exception as e:
        logger.error(f"Fatal error: {e}")


if __name__ == '__main__':
    main()