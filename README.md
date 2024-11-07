# Publisher-Subscriber-System

This project implements a distributed publisher-subscriber system designed to provide component decoupling, scalability, and real-time communication.

## System Architecture

- Broker: Manages topics, subscriptions, and message delivery, ensuring that all Brokers in the network synchronize information.
- Publisher: Responsible for creating topics and publishing messages to the system.
- Subscriber: Subscribes to topics and receives real-time messages.

## Key Features
- Multi-Broker Communication: Broker nodes synchronize with each other to ensure all subscribers receive relevant messages.
- Topic Management: Supports creating, deleting topics, and subscribing/unsubscribing to topics.
- Fault Tolerance: Automatically removes topics and subscriptions when a publisher or subscriber disconnects to maintain consistency across the system.

## Getting Started

**Starting the Broker**
Run the following command to start the Broker and specify the necessary network port.

```java -jar broker.jar <port>```

**Starting the Publisher**
Run the following command to start the Publisher and connect it to a specified Broker.

```java -jar publisher.jar <broker-ip> <broker-port>```

**Starting the Subscriber**
Run the following command to start the Subscriber and connect it to a specified Broker.

```java -jar subscriber.jar <broker-ip> <broker-port>```
