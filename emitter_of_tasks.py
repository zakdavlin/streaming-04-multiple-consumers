"""

Creates and sends a task message to the queue each execution.
This process runs and finishes. 

Approach
---------
Work Queues - one task producer / many workers sharing work.


Multiple Terminals
------------------

1. [Green] Execute this producer several times. 

2. [Red] Run the consumer continously.


"""

import pika
import sys
import webbrowser

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

offer_rabbitmq_admin_site()

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

channel.queue_declare(queue="task_queue", durable=True)

message = " ".join(sys.argv[1:]) or "Task: Someone take on this task."

channel.basic_publish(
    exchange="",
    routing_key="task_queue",
    body=message,
    properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
)

print(f" [x] Sent {message}")

connection.close()


