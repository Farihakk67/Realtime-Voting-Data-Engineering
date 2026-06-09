import json  # JSON data parse karne ke liye (Kafka messages handle)
from kafka import KafkaConsumer, KafkaProducer  # Kafka se data lene aur bhejne ke liye
from collections import defaultdict  # Automatic dictionary creation ke liye (votes count store)
import time  # Time delay aur sleep ke liye (processing control)
from datetime import datetime  # Current timestamp ke liye (log aur data timestamp)

# Initialize Kafka connections
consumer = KafkaConsumer('votes_topic', 
                        bootstrap_servers='localhost:9092',
                        auto_offset_reset='earliest',
                        group_id='python-aggregator',
                        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Storage for aggregated data
candidate_votes = defaultdict(int)
location_counts = defaultdict(int)

print("=" * 60)
print("Python Real-time Vote Aggregator Started!")
print("Waiting for votes from votes_topic...")
print("=" * 60)

try:
    while True:
        # Poll for new messages
        messages = consumer.poll(timeout_ms=1000)
        
        if messages:
            for topic_partition, message_list in messages.items():
                for message in message_list:
                    try:
                        vote = message.value
                        
                        # Extract data
                        candidate_id = vote['candidate_id']
                        candidate_name = vote['candidate_name']
                        state = vote.get('address', {}).get('state', 'Unknown')
                        
                        # Update counts
                        candidate_votes[candidate_id] += 1
                        location_counts[state] += 1
                        
                        # Prepare candidate aggregation
                        candidate_data = {
                            'candidate_id': candidate_id,
                            'candidate_name': candidate_name,
                            'party_affiliation': vote.get('party_affiliation', 'Unknown'),
                            'total_votes': candidate_votes[candidate_id],
                            'photo_url': vote.get('photo_url', '')
                        }
                        
                        # Prepare location aggregation
                        location_data = {
                            'state': state,
                            'count': location_counts[state]
                        }
                        
                        # Send to Kafka
                        producer.send('aggregated_votes_per_candidate', candidate_data)
                        producer.send('aggregated_turnout_by_location', location_data)
                        
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ {candidate_name}: {candidate_votes[candidate_id]} votes | Location: {state}")
                        
                    except Exception as e:
                        print(f"Error processing vote: {e}")
                        continue
        
        # Flush producer to ensure messages are sent
        producer.flush()
        time.sleep(0.1)
        
except KeyboardInterrupt:
    print("\n" + "=" * 60)
    print("Aggregator stopped by user.")
    print("=" * 60)
except Exception as e:
    print(f"\nFatal error: {e}")
finally:
    consumer.close()
    producer.close()