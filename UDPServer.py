
import socket
from kafka import KafkaProducer
from json import dumps
from time import sleep

UDP_IP_ADDRESS = "127.0.0.1"
UDP_PORT_NO = 1234

serverSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
serverSock.bind((UDP_IP_ADDRESS, UDP_PORT_NO))

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],key_serializer=lambda x:dumps(x).encode('utf-8'),value_serializer=lambda x:dumps(x).encode('utf-8'))

while True:
    data, addr = serverSock.recvfrom(1024)
    #str=print ("Message: " + data.decode())
    f = open("test.txt","a+")
    str = data.decode()
    f.write("Message:%s.\n" %str)
    print("Message:"+str)
    producer.send('server',key = 'Message', value = str)
    print("posted successfully.")
    f.close()




