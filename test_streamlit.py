from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

candidates = [
    {
        'candidate_id': '8326f57e-fc71-49fd-b905-992354db7b62',
        'candidate_name': 'Lewis Martin',
        'party_affiliation': 'Management Party',
        'total_votes': 150,
        'photo_url': 'https://randomuser.me/api/portraits/men/18.jpg'
    },
    {
        'candidate_id': 'cf5ded17-3d54-414e-92e2-d6cf828b2de8',
        'candidate_name': 'Faith Richards',
        'party_affiliation': 'Savior Party',
        'total_votes': 120,
        'photo_url': 'https://randomuser.me/api/portraits/women/92.jpg'
    },
    {
        'candidate_id': '570c97c2-c2bb-4582-be30-f6b62d945096',
        'candidate_name': 'Stephen Gonzalez',
        'party_affiliation': 'Tech Republic Party',
        'total_votes': 180,
        'photo_url': 'https://randomuser.me/api/portraits/men/68.jpg'
    }
]

for candidate in candidates:
    producer.send('aggregated_votes_per_candidate', json.dumps(candidate).encode('utf-8'))
    print(f"Sent: {candidate['candidate_name']} - {candidate['total_votes']} votes")
    time.sleep(1)

print("✅ All test candidates sent to Kafka!")