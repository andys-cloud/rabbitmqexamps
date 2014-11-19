'''
Created on 2014-11-19

@author: yunling
'''

import pika
 
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel=connection.channel()
channel.queue_declare(queue="rpc_queue")

def fib(n):
    if n == 0:
        return 0
    elif n == 1 :
        return 1
    else :
        return fib(n-1) + fib(n-2)

def on_request(ch,method,properties,body):
    n = int(body)
    response = fib(n)
    print '[exec: fib(%r) = %r]' %(n,response)
    ch.basic_publish(exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                               correlation_id = properties.correlation_id ),
                    body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    

channel.basic_qos(prefetch_count=1)    
channel.basic_consume(on_request,
                      queue='rpc_queue',
                      no_ack=False)   
 
print " [x] Awaiting RPC requests"
channel.start_consuming()
