'''
Created on 2014-11-19

@author: yunling
'''
import pika
import uuid

class FibonacciRpcClient():
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.connection.channel()
        
        result = self.channel.queue_declare(exclusive=True)
        self.queue_name = result.method.queue
        
        self.channel.basic_consume(self.on_response,
                                   queue=self.queue_name,
                                   no_ack=True)
        
    def on_response(self,ch,method,properties,body):
        if self.correlation_id == properties.correlation_id:
            self.response = body
            
    def call(self,n):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key="rpc_queue",
                                   properties=pika.BasicProperties(
                                            reply_to=self.queue_name,
                                            correlation_id=self.correlation_id),
                                   body=str(n))
        
        while self.response is None:
            self.connection.process_data_events()
        return int(self.response)
    
fibonacci_rpc = FibonacciRpcClient()

print " [x] Requesting fib(30)"
response = fibonacci_rpc.call(30)
print " [.] Got %r" % (response,)    
        
