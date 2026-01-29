import json
from time import time, sleep
from kafka import KafkaProducer
import pandas as pd

data = pd.read_csv('data_grad.csv', sep=',')
#print(data.iterrows())
KAFKA_BROKER = 'localhost:9092'
OUTPUT_TOPIC = 'topicBD'


producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
start_time = time()
TIMEOUT = 30 # seconds

for index, row in data.iterrows():
    if time() - start_time > TIMEOUT:
        print("Timeout reached. Stopping producer.")
        break
    message = {
        'Curricular units 2nd sem (approved)': row['Curricular units 2nd sem (approved)'],
        'Curricular units 2nd sem (grade)': row['Curricular units 2nd sem (grade)'],
        'Curricular units 1st sem (approved)': row['Curricular units 1st sem (approved)'],
        'Curricular units 1st sem (grade)': row['Curricular units 1st sem (grade)'],
        'Tuition fees up to date': row['Tuition fees up to date'],
        'Scholarship holder': row['Scholarship holder'],
        'Age at enrollment': row['Age at enrollment'],
        'Debtor': row['Debtor'],
        'Gender': row['Gender'],
        'Application mode': row['Application mode'],
    }
    producer.send(OUTPUT_TOPIC, value=message)
    message = row.to_dict()
    print(message)
    sleep(1)

producer.flush()
print("All messages sent.")