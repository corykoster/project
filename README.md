# project challenge

Cory Koster

This challenge is to identify IP addresses from a server log that are part of a DDOS attack, if one exists. Once found, the IP address will be written to a file to be processed further (such processing is beyond the scope of this challenge).

To determine if an IP address is part of a DDOS attack, each IP from the file along with the datetime it reached the server is analyzed. Frequency of the IP is captured along with time between appearances. If the time between appearances is less than 20 seconds and the IP had appeared more than 10 times, it is deemed to be part of the DDOS attack. At this point the IP is written to a file and also stored in a list that tracks which IP addresses have been written to the file in order to prevent duplicates i.

The overall process is for a python script to read data from a local file, act as a producer, and publish the message to Kafka. A different python script is designed to consume messages from the same Kafka server. Normally these processes would run indefinitely, publishing and consuming messages as they go, independent from each other. In this project, both the producer and consumer are independent of each other, but are designed in a way to terminate their connection to Kafka when they finish publishing/consuming their respective messages.
