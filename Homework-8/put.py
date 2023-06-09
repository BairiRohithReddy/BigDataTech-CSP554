from json import dumps
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
p = {
"MYID": "A20526972",
"MYNAME": "Rohith Reddy Bairi",
"MYEYECOLOR": "Black"
}
for k in p.keys():
	bkeys =bytes(k, 'utf-8')
	bvalue=bytes(p[k], 'utf-8')
	producer.send('sample',value = bvalue,key=bkeys)

producer.close()
