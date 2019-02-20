# Cory Koster

# This file is used to send log data from a local directory file
# to a Kafka topic. In essence, this file acts as the message producer
# and is set to deliver a new message (i.e. one line from file) continuously.

from kafka import KafkaConsumer, KafkaProducer
import time

# uncomment the following 2 lines to include debugging
#import logging
#logging.basicConfig(level=logging.DEBUG)


# connect to kafka as producer and return the instance
def producer_connect_kafka():
    
    # initialize connector
    connector = None
    
    # try to connect to server and return connector
    # if successful. If not, print the exception
    try:
        
        # bootstrap to kafka and print successful message
        connector = KafkaProducer(bootstrap_servers='192.168.1.30:9092')
        print('kafka producer connection successful')
    
    except Exception as e:
        print('Exception while connecting to Kafka')
        print(str(e))

    finally:
        return connector


# method to publish the file message to the kafka topic
def publish_message(producer, topic, value):
    
    # try to send the message to the kafka topic
    # if it fails, print the exception message
    try:
        
        # convert message value into byte format for kafka
        value_bytes = bytes(value, encoding='utf-8')
        
        # send the encoded message to the topic
        producer.send(topic, value=value_bytes)

    except Exception as e:
        print('Exception in publishing message')
        print(str(e))


# main process
if __name__ == '__main__':
    
    # kafka topic
    topic = 'server_log'
    
    # call method to connect to kafka and receive the connected producer
    producer = producer_connect_kafka()
    
    # try to open/read from local log file
    # if it fails, print the exception
    try:
        
        # with the file open in read mode, loop through each
        # line in the file, publishing as it goes
        with open("/Users/cory/Desktop/phdata/Project - Developer - apache-access-log (4).txt", "r") as file:
            
            for line in file:
                
                # call method to publish message from file
                publish_message(producer, topic, line)

        # close the file and print successful message
        file.close()
        
        # build in a delay that allows the producer to finish publishing before closing connection
        time.sleep(20)

        # successful message
        print("Success - file has been read and messages published.")


    except Exception as e:
        print('Exception in publishing message')
        print(str(e))

