from kafka import KafkaConsumer
consumer = KafkaConsumer(
'sample',
bootstrap_servers=['localhost:9092'],
auto_offset_reset='earliest',
consumer_timeout_ms=10000,
group_id='my-group')
for message in consumer:
	print("key=%s value=%s" % (message.key.decode('utf-8'),message.value.decode('utf8')))
consumer.close()