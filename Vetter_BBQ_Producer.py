"""
    Module 5 - Creating a Producer
    This program sends a message to a queue on the RabbitMQ server.
    The csv file contains date, smoker temp, meat 1(temp), and meat 2(temp). 
    The csv_rabbit_reader function offers a way to stream messages to and from the queues of Rabbit MQ. 
    However, I have built onto the template that I created from my 4th module. 
    Please see the below additions to account for 3 different queues for each of the columns in the csv file. 

    

    Author: Nic Vetter
    Date: May 31, 2024

"""

import pika
import sys
import webbrowser
import csv
import datetime
import time
import struct

# My logging setup - NV
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# This is an offering message for easy access to the RabbitMQ Queue page.
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

# The below function is standard from our prior modules. The function will create a message to be sent to a specific queue. 
def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name/IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # creating a channel with the connection
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        # Include the queue name in the log message
        print(f" [x] Sent message to {queue_name}")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()



# Reading in the csv file and sending to Rabbit MQ. 
# This is the part where I will have to build onto the prior module code.
# The below code will specify which column/row of data will go to each queue.
# This is also the first time I have ever used the "struct" module! - NV

# CSV_Smoker_Reader is my main funtion within this python file. 
# It will read the CSV file and sort it into certain queues based on the row of the csv.
def csv_smoker_reader():
    with open("smoker-temps.csv", newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader) #This will skip the header row

        for smoke_row in reader:
            time_of_smoke = smoke_row[0]
            temp_of_smoker = smoke_row[1]
            foodA_temp = smoke_row[2]
            foodB_temp = smoke_row[3]
            # I was having trouble getting my time_of-smoke(timestamp) to show correctly. I finally figured it out by looking at different repos. 
            # I needed to convert to Unix and after that, my producer worked great!
            # This sets up the file to be parsed into the matching CSV format
            time_of_smoke = datetime.datetime.strptime(time_of_smoke, '%m/%d/%y %H:%M:%S').timestamp()

            # The below will read the CSV rows and send the correct data to the correct Rabbit Queue. 
            # I have already established parameters in the "smoke_row" code above. 
            # The below code acts as a filter for the csv_smoker_reader. 
            if temp_of_smoker:
                message = struct.pack('!df', time_of_smoke, float(temp_of_smoker))
                send_message("localhost","01-smoker", message)
            if foodA_temp:
                message = struct.pack('!df', time_of_smoke, float(foodA_temp))
                send_message("localhost","02-food-A", message) 
            if foodB_temp:
                message = struct.pack('!df', time_of_smoke, float(foodB_temp))
                send_message("localhost","03-food-B", message) 

                time.sleep(30) # Ensures the programs delays 30 seconds during the reading process. 

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below - NV
if __name__ == "__main__":  
    # Proposes to open the RabbitMQ site
    offer_rabbitmq_admin_site()

    # Calling the send function to get the tasks sent to the correct queues
    csv_smoker_reader()
