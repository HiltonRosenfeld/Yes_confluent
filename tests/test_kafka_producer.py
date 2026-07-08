# Smoke test: verifies Confluent Cloud Kafka credentials by producing one test message.
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import publisher

if __name__ == '__main__':
    result = {'ok': False, 'err': None}

    def _cb(err, msg):
        if err is None:
            result['ok'] = True
        else:
            result['err'] = str(err)

    producer = publisher.get_producer()
    producer.produce(publisher.TOPIC, key=b'test', value=b'{"test":true}', callback=_cb)
    producer.flush(timeout=10)

    if result['ok']:
        print("Kafka producer OK. Message delivered.")
        sys.exit(0)
    else:
        print(f"Kafka producer FAILED: {result['err']}")
        sys.exit(1)
