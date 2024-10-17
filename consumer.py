from kafka import KafkaConsumer
from kafka.errors import KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json

# Kafka configuration
bootstrap_servers = 'xxxx'
topic_name = 'mycar'

# Create a token provider for MSK IAM authentication
class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-west-2')
        return token
# Create a Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=MSKTokenProvider(),
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # Start reading from the beginning of the topic
    enable_auto_commit=False,  # Disable auto-commit to ensure we count all messages
    consumer_timeout_ms=10000  # Stop iteration after 10 seconds of no messages
)

# Initialize message counter
message_count = 0

try:
    # Iterate through messages in the topic
    for message in consumer:
        timestamp = json.loads(message.value)['@timestamp']
        print(f"message count:" + str(message_count))
        print(timestamp)
        message_count += 1
        
    print(f"Total messages in topic '{topic_name}': {message_count}")

except KafkaError as e:
    print(f"An error occurred: {e}")

finally:
    # Close the consumer
    consumer.close()

