# Distributed Publisher-Subscriber System! 📬

This Java-based project enables real-time communication between multiple publishers and subscribers using a network of Brokers to handle message routing with flexibility and efficiency.

## System Design
- **Decoupled Architecture** <br>
  The system decouples publishers and subscribers through a network of Brokers, allowing for asynchronous communication and reducing direct dependencies between components.
- **Multi-Broker Synchronization** <br>
  Brokers communicate with each other to ensure message consistency across the network, so subscribers can always receive relevant messages, regardless of which Broker they're connected to.
- **Fault Tolerance & Scalability** <br>
  Fault-tolerant mechanisms are in place to handle disconnections of publishers or subscribers, automatically removing related topics and subscriptions to maintain system consistency. Additionally, using a thread-per-client model ensures that each connection is handled independently, enhancing scalability.
- **TCP Socket Communication** <br>
  The system uses TCP sockets for reliable, connection-oriented communication between publishers, subscribers, and brokers. This design choice ensures that messages are reliably delivered and that real-time updates are consistently synchronized across all brokers.

## 📤 Publisher Functionality
- Create Topics
- Publish Messages
- Show Subscriber Count
- Delete Topics

## 📥 Subscriber Functionality
- List Topics
- Subscribe to Topics
- Receive Real-Time Messages
- Unsubscribe from Topics


## 💡 Getting Started
Make sure you have JDK 21.0.3 installed. Then, follow these steps:

1. Download the following files and place them in a folder:
   - `broker.jar`
   - `publisher.jar`
   - `subscriber.jar`

2. Navigate to the folder in your terminal:
   ```bash
   cd /path/to/your/folder
   ```
3. Start the Broker by specifying the port:
   ```bash
   java -jar broker.jar <port>
   ```
4. Start the Publisher and connect it to a Broker:
   ```bash
   java -jar publisher.jar <broker-ip> <broker-port>
   ```
5. Start the Subscriber and connect it to a Broker:
   ```bash
   java -jar subscriber.jar <broker-ip> <broker-port>
   ```

Feel free to reach out with any questions or feedback.👋
