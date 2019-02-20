# Cory Koster

# This program is used to get data from a Kafka topic
# and detect whether an attack occurred that was part
# of the DDOS attack. In essence, this program acts
# as the consumer. When/if found, the record will
# be written to a file for further processing.

from kafka import KafkaConsumer
import threading, logging, time
from datetime import datetime
import multiprocessing as mp

# uncomment the following 2 lines to include debugging
#import logging
#logging.basicConfig(level=logging.DEBUG)

# method to consume messages from kafka topic
# and process them for determining attack
def consume_messages(topic):
    
    # for tracking time differences between corresponding records
    time_diff = 0
    
    # empty dictionarys to hold ip addresses as well as time they hit the server
    ip_dictionary = {}
    
    # list to hold ip addresses written to file
    ips_to_process = []
    
    # try to retrieve and process messages from kafka
    try:
        
        # keep the connection open indefinitely to continuously get messages
        while True:
            
            # make the consumer connection, using the earliest offset and bootstrapping the kafka broker
            consumer = KafkaConsumer(topic, auto_offset_reset='earliest', bootstrap_servers='192.168.1.30:9092')
            
            # loop through the set of messages retrieved by the consumer
            for message in consumer:
                
                # decode messages because kafka sends/receives byte format
                # split the message on a space in order to get ip address (list index = 0)
                # and the time string (list index = 3, starting at string index = 1)
                decoded_message = message.value.decode('utf-8').split(' ')
                ip_addr = decoded_message[0]
                ip_time_string = decoded_message[3][1:]
                
                # datetime string format -- 25/May/2015:23:11:17
                ip_time_date = datetime.strptime(ip_time_string, '%d/%b/%Y:%H:%M:%S')
    
                # check if the ip address is already present in the dictionary
                # if present, check the last time it hit the server (time difference in seconds)
                if(ip_addr in ip_dictionary):
                    
                    # get the time difference
                    time_diff = (ip_time_date - ip_dictionary.get(ip_addr)[0]).total_seconds()
                    
                    # store latest time ip hit the server
                    ip_dictionary[ip_addr][0] = ip_time_date
                    
                    # increment appearances of ip
                    ip_dictionary[ip_addr][1] += 1
                    
                    # check if: -the time difference is less than 20 seconds
                    #           -the ip has occurred more than 10 times
                    #           -ip has not been written to the file already
                    # if true, then append ip address to file
                    if(time_diff <= 20 and ip_dictionary[ip_addr][1] >= 10 and ip_addr not in ips_to_process):
                        
                        # add ip address to the list of ips written to the file
                        ips_to_process.append(ip_addr)
                        
                        # write the ip address to the file for processing (file created if doesn't exist)
                        with open("process_ips.txt", "a") as ip_file:
                            ip_file.write(ip_addr + "\n")
            
                # else, add the ip to the dictionary with the time and first appearance
                else:
                    ip_dictionary[ip_addr] = [ip_time_date, 1]
        
                # normally the consumer wouldn't be closed and would continue to consume messages.
                # in this demo, the consumer is closed when the file is done being processed (offset equals number of records in file)
                if(message.offset == 163415):
                    print("IPs to process have been written to file process_ips.txt")
                    consumer.close()
                    print("Consumer is closed")

    # catch and print the exception if an error is caught
    except Exception as e:
        print('Exception in consuming message')
        print(str(e))



# main process
if __name__ == '__main__':

    # kafka topic
    topic = 'server_log'
    
    # call the method to connect to kafka and process messages
    consume_messages(topic)

