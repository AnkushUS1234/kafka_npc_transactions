#!/usr/bin/env python3
"""
Quick verification script to test Kafka and MongoDB connections
"""

import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def test_imports():
    """Test if required packages can be imported"""
    logger.info("🔍 Testing imports...")
    
    try:
        from confluent_kafka import Producer, Consumer
        logger.info("   ✅ confluent_kafka imported successfully")
    except ImportError as e:
        logger.error(f"   ❌ Failed to import confluent_kafka: {e}")
        return False
    
    try:
        from pymongo import MongoClient
        logger.info("   ✅ pymongo imported successfully")
    except ImportError as e:
        logger.error(f"   ❌ Failed to import pymongo: {e}")
        return False
    
    return True

def test_kafka_connection():
    """Test Kafka connection"""
    logger.info("\n🔍 Testing Kafka Connection...")
    
    try:
        from confluent_kafka import Producer
        
        config = {
            'bootstrap.servers': 'pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': 'KK5AE5WBMLIPKT7J',
            'sasl.password': 'cfltAlG85cdkM17dUtfzrE4Ve2lXKVt5ksfW8x9pSnoVP0A+1ZVKjC12GTmfZ8Fw',
            'client.id': 'test-client'
        }
        
        producer = Producer(config)
        logger.info("   ✅ Kafka connection successful")
        producer = None
        return True
        
    except Exception as e:
        logger.error(f"   ❌ Kafka connection failed: {e}")
        return False

def test_mongodb_connection():
    """Test MongoDB connection"""
    logger.info("\n🔍 Testing MongoDB Connection...")
    
    try:
        from pymongo import MongoClient
        
        mongo_uri = 'mongodb+srv://ankush_db_user:WHBr5CO4JJ7cYmw2@clusternpcbank.lnwuht2.mongodb.net/'
        
        # SSL/TLS configuration for MongoDB Atlas
        client = MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=5000,
            tls=True,
            tlsAllowInvalidCertificates=True  # For development/testing
        )
        
        # Attempt to ping the server
        client.admin.command('ping')
        logger.info("   ✅ MongoDB connection successful")
        
        # Check databases
        databases = client.list_database_names()
        logger.info(f"   📊 Available databases: {', '.join(databases[:5])}{'...' if len(databases) > 5 else ''}")
        
        client.close()
        return True
        
    except Exception as e:
        logger.error(f"   ❌ MongoDB connection failed: {e}")
        return False

def main():
    """Run all tests"""
    logger.info("=" * 60)
    logger.info("KAFKA & MONGODB CONNECTION VERIFICATION")
    logger.info("=" * 60)
    
    results = {
        'Imports': test_imports(),
        'Kafka': test_kafka_connection(),
        'MongoDB': test_mongodb_connection()
    }
    
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    
    for test, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"{test:20} {status}")
    
    if all(results.values()):
        logger.info("\n🎉 All connections verified successfully!")
        logger.info("\nYou can now run:")
        logger.info("   python3 banking_producer.py    (Send data to Kafka)")
        logger.info("   python3 mongodb_consumer.py     (Consume from Kafka & write to MongoDB)")
        return 0
    else:
        logger.error("\n⚠️  Some connections failed. Please check your configuration.")
        return 1

if __name__ == '__main__':
    sys.exit(main())
