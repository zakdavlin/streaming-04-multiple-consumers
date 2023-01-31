"""

Creates and sends a task message to the queue each execution.
This process runs and finishes. 
Make tasks harder/longer-running by adding dots at the end of the message.

Approach
---------
Work Queues - one task producer / many workers sharing work.
Had to run batch file to operate rabbitmq connection

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

# call the function defined above
offer_rabbitmq_admin_site()

# create a blocking connection to the RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
# use the connection to create a communication channel
channel = connection.channel()
# use the channel to declare a duyrable queue
# a durable queue will survive a RabbitMQ server restart
# and help ensure messages are processed in order
# messages will not be deleted until the consumer acknowledges
channel.queue_declare(queue="task_queue", durable=True)
# create a message by joining the command line arguments
message = " ".join(sys.argv[1:]) or "First task..."
# publish the message to the queue
channel.basic_publish(
    exchange="",
    routing_key="task_queue",
    body=message,
    properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
)
# tell the user the message was sent
print(f" [x] Sent {message}")
# close the connection to the server
connection.close()
