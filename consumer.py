import json
from time import sleep, time
from joblib import load
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd

random_forest = load('random_forest_model.joblib')
logistic_regression = load('logistic_regression_model.joblib')
gradient_boosting = load('gradient_boosting_model.joblib')

def predict_random_forest(X):
    return random_forest.predict(X)
def predict_logistic_regression(X):
    return logistic_regression.predict(X)
def predict_gradient_boosting(X):
    return gradient_boosting.predict(X)


KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'topicBD'
OUTPUT_TOPIC = 'topic_predictions'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='latest',
    #group_id=f'student_consumer_{int(time())}',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

"""MAIN LOOP"""
start_time = time()
TIMEOUT = 30 # seconds

for message in consumer:
    incoming_json = message.value
    print(f"Received message: {incoming_json}")
    current_time = time()
    if current_time - start_time > TIMEOUT:
        print("Timeout reached. Stopping consumer.")
        break

    try:
        df = pd.DataFrame([incoming_json])

        rf_prediction = predict_random_forest(df)[0]
        lr_prediction = predict_logistic_regression(df)[0]
        gb_prediction = predict_gradient_boosting(df)[0]

        print("\nRAW DATA\n: ", incoming_json, "\n") # Debugging line to check DataFrame structure

        result = {
            'random_forest_prediction': int(rf_prediction),
            'logistic_regression_prediction': int(lr_prediction),
            'gradient_boosting_prediction': int(gb_prediction)
        }

        print(f"Sending prediction: {result}")
        producer.send(OUTPUT_TOPIC, value=result)

    except Exception as e:
        print(f"Error processing message {incoming_json}: {e}")

    sleep(1)

consumer.close()
producer.flush()
print("Consumer stopped and all messages sent.")