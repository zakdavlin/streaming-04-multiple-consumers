"""

Listens for task messages on the queue.
This process runs continously. 

Make as many listening workers as you need 
(start this process in multiple terminals).

Approach
---------
Work Queues - one task producer / many workers sharing work.


Terminal Reminders
------------------

- Use Control c to close a terminal and end a process.

- Use the up arrow to get the last command executed.

"""

import pika
import sys
import os
import time


def listen_for_tasks():
    """ Continuously listen for task messages on a named queue."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    ch = connection.channel()

    def callback(ch, method, properties, body):
        """ Define behavior on getting a message."""
        print(f" [x] Received {body.decode()}")
        time.sleep(body.count(b"."))
        print(" [x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.queue_declare(queue="task_queue", durable=True)
    print(" [*] Waiting for messages. To exit press CTRL+C")

    ch.basic_qos(prefetch_count=1) # Per consumer limit of unaknowledged messages
    ch.basic_consume(queue="task_queue", on_message_callback=callback)

    print(" [*] Ready for work. To exit press CTRL+C")

    ch.start_consuming()


if __name__ == "__main__":
    try:
        listen_for_tasks()

    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
