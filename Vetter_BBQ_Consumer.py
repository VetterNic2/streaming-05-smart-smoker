"""
    This program listens for work messages concurrently. I will have one consumer that has the ability to navigate data in each queue.
    This module calls for 3 queues, but since I am running only one consumer, I will have to set up my call back functions to distinguish between queues.   
    This code uses time, callback and functions with exceptions to receive messages from RabbitMQ. 
    The number of queues also calls for multiple callback functions. 3 to be exact. 
    Below, you will be able to see my queues broken out into each csv row, as well as callback functions for each queue. 
    I am also embedding logging statements instead of print so the user can see the state of the emitting/consuming. 

    Author: Nic Vetter
    Date: June 6-9, 2024

"""

import pika
import sys
import struct
import datetime
import time
from collections import deque
from util_logger import setup_logger

# Set up logging
logger, logname = setup_logger(__file__)

# Define deques for storing temperature readings
smoker_temps = deque(maxlen=5)  # Maximum length for 2.5 minute window
foodA_temps = deque(maxlen=20)  # Maximum length for 10 minute window
foodB_temps = deque(maxlen=20)  # Maximum length for 10 minute window

# The following 3 call backs will be used for the different Queues. It was super cool to set up with the help of my AI assistant.
# I was very surprised how much AI knew about Rabbit MQ related python. It was a huge help throughout this class.  
# I opted to go with logging statements as well because I think it is much easier. 

# Define a callback function for the smoker queue.
# The below callback function shows struct formatting along with a timestamp_str function using the datetime module. 
def smoker_callback(ch, method, properties, body):
    """Callback function for handling messages from the smoker queue."""
    timestamp, temperature = struct.unpack('!df', body)
    timestamp_str = datetime.datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    logger.info(f"Received from smoker queue: {timestamp_str} - Temperature: {temperature}F")
    
    # Add the new temperature reading to the deque
    # This is showing the append method, which we learned about in several classes. 
    # All this is doing is adding a new data point to the deque, so it can be effectively pushed through the streaming pipeline. 
    smoker_temps.append(temperature)
    
    # Check if the temperature drop is 15F or more within the last 2.5 minutes

    if len(smoker_temps) == smoker_temps.maxlen:
        if smoker_temps[0] - temperature >= 15:
    # The below sets up a logging statement to show the user if the smoker temp drops by more than 15F. 
            logger.info("Smoker alert! Temperature dropped by 15F or more in the last 2.5 minutes!")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Food A Queue callback function. 
# This is in the same format as before, but I had to add a food stall scenario. 
# logging is shown correctly as well. 
def foodA_callback(ch, method, properties, body):
    """Callback function for handling messages from the food A queue."""
    timestamp, temperature = struct.unpack('!df', body)
    timestamp_str = datetime.datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    logger.info(f"Received from food A queue: {timestamp_str} - Temperature: {temperature}F")
    
    # Add the new temperature reading to the deque
    foodA_temps.append(temperature)
    
    # Food Stall check!
    if len(foodA_temps) == foodA_temps.maxlen:
        if max(foodA_temps) - min(foodA_temps) <= 1:
            logger.info("Food A stall alert! Temperature change is 1F or less in the last 10 minutes!")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Define a callback function for the food B queue
# This is in the same format as before, but I had to add a food stall scenario. 
# logging is shown correctly as well. 
def foodB_callback(ch, method, properties, body):
    """Callback function for handling messages from the food B queue."""
    timestamp, temperature = struct.unpack('!df', body)
    timestamp_str = datetime.datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    logger.info(f"Received from food B queue: {timestamp_str} - Temperature: {temperature}F")
    
    # Add the new temperature reading to the deque
    foodB_temps.append(temperature)
    
    # Food Stall check!
    if len(foodB_temps) == foodB_temps.maxlen:
        if max(foodB_temps) - min(foodB_temps) <= 1:
            logger.info("Food B stall alert! Temperature change is 1F or less in the last 10 minutes!")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# The below is the main function for the consumer. 
# You can see the connection to the local host and declaration of queues. 
def consume_messages():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Declare each queue
        queues = ["01-smoker", "02-food-A", "03-food-B"]
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_qos(prefetch_count=1)

        # Start consuming messages from each queue
        channel.basic_consume(queue="01-smoker", on_message_callback=smoker_callback, auto_ack=False)

        channel.basic_consume(queue="02-food-A", on_message_callback=foodA_callback, auto_ack=False)

        channel.basic_consume(queue="03-food-B", on_message_callback=foodB_callback, auto_ack=False)

        print(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()

# Logging Error message so the user is notified of any errors. 
    except Exception as e:
        logger.error(f"ERROR: {e}")
        sys.exit(1)

# Running the consumer and calling functions.
if __name__ == "__main__":
    consume_messages()
