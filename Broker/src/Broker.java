import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Broker {
    private int port;
    private List<String> otherBrokers;
    private Map<String, String> topics = new HashMap<>(); // topic_id -> topic_name
    private Map<String, Set<String>> topicPublishers = new HashMap<>(); // topicId -> publisher names
    private Map<String, List<Socket>> topicSubscribers = new HashMap<>(); // topicId -> subscriber
    private Map<Socket, Set<String>> subscriberTopics = new HashMap<>(); // subscriber socket -> set of topicIds
    private Map<Socket, Set<String>> publisherTopics = new HashMap<>(); // publisher socket -> set of topicIds
    private List<Socket> connectedBrokers = new ArrayList<>();
    private ServerSocket serverSocket;

    public Broker(int port, List<String> otherBrokers) {
        this.port = port;
        this.otherBrokers = otherBrokers;
    }

    private String getTimestamp() {
        return new java.text.SimpleDateFormat("dd/MM HH:mm:ss").format(new Date());
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println(getTimestamp() + " Broker started on port " + port);

        // Start connecting to other brokers
        new Thread(this::connectToOtherBrokers).start();

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(() -> handleClient(clientSocket)).start();
        }
    }

    private void connectToOtherBrokers() {
        for (String brokerAddress : otherBrokers) {
            try {
                String[] parts = brokerAddress.split(":");
                String brokerIp = parts[0];
                int brokerPort = Integer.parseInt(parts[1]);

                Socket brokerSocket = new Socket(brokerIp, brokerPort);
                connectedBrokers.add(brokerSocket);
                System.out.println(getTimestamp() + " Connected to broker: " + brokerIp + ":" + brokerPort);

                sendNewBrokerInfo(brokerSocket);

                new Thread(() -> handleClient(brokerSocket)).start();
            } catch (IOException e) {
                System.out.println(getTimestamp() + " Failed to connect to broker: " + brokerAddress);
            }
        }
    }

    // Notify other brokers of the new broker and propagate the connection
    private void sendNewBrokerInfo(Socket brokerSocket) throws IOException {
        PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
        out.println("newbroker " + InetAddress.getLocalHost().getHostAddress() + ":" + port);
    }

    private void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String command;
            while (true) {

                if (clientSocket.isClosed() || !clientSocket.isConnected()) {
                    handleClientCrash(clientSocket);
                    break;
                }

                if ((command = in.readLine()) != null) {
                    String[] parts = command.split(" ");

                    switch (parts[0]) {
                        case "create":
                            String topicId = parts[1];
                            String topicName = parts[2];
                            String publisherName = parts[3];
                            createTopic(topicId, topicName, publisherName, clientSocket, out);
                            forwardCreateToBrokers(topicId, topicName, publisherName, clientSocket);
                            break;
                        case "forwardCreateToBrokers":
                            topicId = parts[1];
                            topicName = parts[2];
                            publisherName = parts[3];
                            createTopic(topicId, topicName, publisherName, clientSocket, out);
                            break;
                        case "publish":
                            topicId = parts[1];
                            publisherName = parts[2];
                            if (topicPublishers.containsKey(topicId)
                                    && topicPublishers.get(topicId).contains(publisherName)) {
                                String message = command.substring(command.indexOf(parts[3]));
                                publishMessage(topicId, message);
                                forwardMessageToBrokers(topicId, message, clientSocket);
                                out.println("success");
                            } else {
                                out.println("error You are not the creator of this topic.");
                            }
                            break;
                        case "forwardPublishToBrokers":
                            topicId = parts[1];
                            String message = command.substring(command.indexOf(parts[2]));
                            publishMessage(topicId, message);
                            break;
                        case "showAll":
                            publisherName = parts[1];
                            showTopicsAndSubscribers(null, publisherName, out);
                            break;
                        case "show":
                            topicId = parts[1];
                            String publisher = parts[2];
                            showTopicsAndSubscribers(topicId, publisher, out);
                            break;
                        case "delete":
                            topicId = parts[1];
                            publisherName = parts[2];
                            if (topicPublishers.containsKey(topicId)
                                    && topicPublishers.get(topicId).contains(publisherName)) {
                                deleteTopic(topicId);
                                forwardDeleteToBrokers(topicId);
                                out.println("success");
                            } else {
                                out.println("error You are not the creator of this topic.");
                            }
                            break;
                        case "forwardDeleteToBrokers":
                            topicId = parts[1];
                            deleteTopic(topicId);
                            break;
                        case "list":
                            listTopics(out);
                            break;
                        case "subscribe":
                            topicId = parts[1];
                            subscribeToTopic(topicId, clientSocket, out);
                            forwardSubscribeToBrokers(topicId, clientSocket);
                            break;
                        case "forwardSubscribeToBrokers":
                            topicId = parts[1];
                            subscribeToTopic(topicId, clientSocket, out);
                            break;
                        case "current":
                            listCurrentSubscriptions(clientSocket, out);
                            break;
                        case "unsubscribe":
                            topicId = parts[1];
                            unsubscribeFromTopic(topicId, clientSocket, out);
                            forwardUnsubscribeToBrokers(topicId, clientSocket);
                            break;
                        case "forwardUnubscribeToBrokers":
                            topicId = parts[1];
                            unsubscribeFromTopic(topicId, clientSocket, out);
                            break;
                        case "newbroker":
                            String brokerInfo = parts[1];
                            if (!isBrokerAlreadyConnected(brokerInfo)) {
                                connectToNewBroker(brokerInfo);
                            }
                            break;
                        default:
                            out.println("error Unknown command");
                            break;
                    }
                } else {
                    handleClientCrash(clientSocket);
                    break;
                }
            }
        } catch (IOException e) {
            handleClientCrash(clientSocket);
        }
    }

    // Handle client crash for both Publisher and Subscriber
    private void handleClientCrash(Socket clientSocket) {
        System.out.println("Client disconnected: " + clientSocket);

        // Handle publisher crash (delete all its topics and forward to other brokers)
        if (publisherTopics.containsKey(clientSocket)) {
            Set<String> publisherTopicIds = publisherTopics.remove(clientSocket);
            if (publisherTopicIds != null) {
                for (String topicId : publisherTopicIds) {
                    deleteTopic(topicId);
                    forwardDeleteToBrokers(topicId);
                }
            }
        }

        // Handle subscriber crash (unsubscribe from all topics and forward to other brokers)
        if (subscriberTopics.containsKey(clientSocket)) {
            Set<String> subscribedTopicIds = subscriberTopics.remove(clientSocket);
            if (subscribedTopicIds != null) {
                for (String topicId : subscribedTopicIds) {
                    unsubscribeFromTopic(topicId, clientSocket, new PrintWriter(System.out));
                    forwardUnsubscribeToBrokers(topicId, clientSocket);
                }
            }
        }
    }

    private boolean isBrokerAlreadyConnected(String brokerInfo) {
        return otherBrokers.contains(brokerInfo);
    }

    private void connectToNewBroker(String brokerInfo) {
        try {
            String[] parts = brokerInfo.split(":");
            String brokerIp = parts[0];
            int brokerPort = Integer.parseInt(parts[1]);

            Socket newBrokerSocket = new Socket(brokerIp, brokerPort);
            connectedBrokers.add(newBrokerSocket);
            System.out.println(getTimestamp() + " Connected to new broker at " + brokerIp + ":" + brokerPort);
        } catch (IOException e) {
            System.out.println(getTimestamp() + " Failed to connect to new broker: " + brokerInfo);
        }
    }

    // Publisher Commands
    private synchronized void createTopic(String topicId, String topicName, String publisherName, Socket publisherSocket, PrintWriter out) {
        if (topics.containsKey(topicId)) {
            out.println("error Topic ID " + topicId + " is already in use.");
            System.out.println("Topic creation failed: " + topicId + " is already in use.");
        } else {
            topics.put(topicId, topicName);
            topicPublishers.computeIfAbsent(topicId, k -> new HashSet<>()).add(publisherName);
            publisherTopics.computeIfAbsent(publisherSocket, k -> new HashSet<>()).add(topicId);
            System.out.println("Topic created: " + topicId + " " + topicName + " by " + publisherName);
            out.println("success");
        }
    }

    private void forwardCreateToBrokers(String topicId, String topicName, String publisherName, Socket origin) throws IOException {
        for (Socket broker : connectedBrokers) {
            if (!broker.equals(origin)) {
                PrintWriter out = new PrintWriter(broker.getOutputStream(), true);
                out.println("forwardCreateToBrokers " + topicId + " " + topicName + " " + publisherName);
            }
        }
    }

    private synchronized void publishMessage(String topicId, String message) throws IOException {
        String topicName = topics.get(topicId);
        if (topicSubscribers.containsKey(topicId)) {
            for (Socket subscriber : topicSubscribers.get(topicId)) {
                PrintWriter out = new PrintWriter(subscriber.getOutputStream(), true);
                String formattedMessage = String.format("%s %s:%s: %s",
                    getTimestamp(), topicId, topicName, message);
                out.println(formattedMessage + "\n\n");
                out.flush();
            }
        }
        System.out.println("Received new message for topic: " + topicId + " " + message);
    }

    private void forwardMessageToBrokers(String topicId, String message, Socket origin) throws IOException {
        for (Socket broker : connectedBrokers) {
            if (!broker.equals(origin)) {
                PrintWriter out = new PrintWriter(broker.getOutputStream(), true);
                out.println("forwardPublishToBrokers " + topicId + " " + message);
            }
        }
    }

    private synchronized int showSubscriberCount(String topicId) {
        List<Socket> subscribers = topicSubscribers.get(topicId);
        if (subscribers != null) {
            return subscribers.size();
        } else {
            return 0;
        }
    }

    private void showTopicsAndSubscribers(String topicId, String publisherName, PrintWriter out) {
        if (topicId != null) {
            if (topics.containsKey(topicId) && topics.get(topicId) != null && topicPublishers.get(topicId).contains(publisherName)) {
                int count = showSubscriberCount(topicId);
                out.println(topicId + " " + topics.get(topicId) + " " + count);
            } else {
                out.println("exception Topic not found or you are not the publisher of this topic.");
            }
        } else {
            boolean hasCreatedTopics = false;
            for (Map.Entry<String, String> entry : topics.entrySet()) {
                String id = entry.getKey();
                String name = entry.getValue();
                if (name != null && topicPublishers.get(id).contains(publisherName)) {
                    hasCreatedTopics = true;
                    int count = showSubscriberCount(id);
                    out.println(id + " " + name + " " + count);
                }
            }
            if (!hasCreatedTopics) {
                out.println("exception Haven't created any topics.");
            }
            out.println("END_OF_RESPONSE");
        }
    }

    private synchronized void deleteTopic(String topicId) {
        String topicName = topics.get(topicId);

        if (topicSubscribers.containsKey(topicId)) {
            List<Socket> subscribers = topicSubscribers.get(topicId);
            for (Socket subscriber : subscribers) {
                try {
                    PrintWriter out = new PrintWriter(subscriber.getOutputStream(), true);
                    String deleteMessage = String.format("%s %s:%s: Topic is deleted",
                        getTimestamp(), topicId, topicName);
                    out.println(deleteMessage + "\n\n");
                    out.flush();

                    Set<String> subscribedTopics = subscriberTopics.get(subscriber);
                    if (subscribedTopics != null) {
                        subscribedTopics.remove(topicId);
                        if (subscribedTopics.isEmpty()) {
                            subscriberTopics.remove(subscriber);
                        }
                    }

                } catch (IOException e) {
                    System.out.println("Failed to notify subscriber about topic deletion: " + topicId);
                }
            }
        }
        topics.remove(topicId);
        topicSubscribers.remove(topicId);
        System.out.println("Topic deleted: " + topicId);
    }

    private void forwardDeleteToBrokers(String topicId) {
        for (Socket broker : connectedBrokers) {
            try {
                PrintWriter out = new PrintWriter(broker.getOutputStream(), true);
                out.println("forwardDeleteToBrokers " + topicId);
            } catch (IOException e) {
                System.out.println("Failed to forward delete topic: " + topicId + " to a broker");
            }
        }
    }

    // Subscriber Commands
    private void listTopics(PrintWriter out) {
        if (topics.isEmpty()) {
            out.println("exception No topics available. \n\n");
        } else {
            for (Map.Entry<String, String> entry : topics.entrySet()) {
                String topicId = entry.getKey();
                String topicName = entry.getValue();
                if (topicName != null) {
                    Set<String> publishers = topicPublishers.get(topicId);
                    String publisherList = String.join(", ", publishers);
                    out.println(topicId + " " + topicName + " " + publisherList);
                }
            }
            out.println("\n\n");
        }
        System.out.println("Listed topics to subscriber");
    }

    private synchronized void subscribeToTopic(String topicId, Socket socket, PrintWriter out) {
        if (topics.containsKey(topicId)) {

            topicSubscribers.putIfAbsent(topicId, new ArrayList<>());
            topicSubscribers.get(topicId).add(socket);

            subscriberTopics.putIfAbsent(socket, new HashSet<>());
            subscriberTopics.get(socket).add(topicId);

            out.println("success\n\n");
            System.out.println("Subscriber subscribed to topic: " + topics.get(topicId) + " [ID: " + topicId + "]");
        } else {
            out.println("error Topic ID not found. \n\n");
            System.out.println("Subscriber subscribed to topic but the topic ID not found.");
        }
    }

    private void forwardSubscribeToBrokers(String topicId, Socket origin) throws IOException {
        for (Socket broker : connectedBrokers) {
            if (!broker.equals(origin)) {
                PrintWriter out = new PrintWriter(broker.getOutputStream(), true);
                out.println("forwardSubscribeToBrokers " + topicId);
            }
        }
    }

    private synchronized void unsubscribeFromTopic(String topicId, Socket clientSocket, PrintWriter out) {
        if (topicSubscribers.containsKey(topicId)) {
            List<Socket> subscribers = topicSubscribers.get(topicId);
            if (subscribers != null && subscribers.remove(clientSocket)) {
                System.out.println("Subscriber unsubscribed from topic: " + topicId);
                
                Set<String> subscribedTopics = subscriberTopics.get(clientSocket);
                
                if (subscribedTopics != null) {
                    subscribedTopics.remove(topicId);

                    if (subscribedTopics.isEmpty()) {
                        subscriberTopics.remove(clientSocket);
                    }
                }
                
                out.println("success\n\n");
            } else {
                out.println("exception Topic ID was not subscribed.\n\n");
                System.out.println("Subscriber was not subscribed to topic: " + topicId);
            }
        } else {
            out.println("exception Topic ID not found. \n\n");
            System.out.println("Subscriber unsubscribes topic ID not found: " + topicId);
        }
    }
    

    private void forwardUnsubscribeToBrokers(String topicId, Socket origin) {
        for (Socket broker : connectedBrokers) {
            if (!broker.equals(origin)) {
                try {
                    PrintWriter out = new PrintWriter(broker.getOutputStream(), true);
                    out.println("forwardUnubscribeToBrokers " + topicId);
                } catch (IOException e) {
                    System.err.println("[error] Failed to forward unsubscribe for topic: " + topicId + " to broker: " + broker);
                    e.printStackTrace();
                }
            }
        }
    }

    private synchronized void listCurrentSubscriptions(Socket clientSocket, PrintWriter out) {
        Set<String> subscribedTopics = subscriberTopics.get(clientSocket);

        if (subscribedTopics != null && !subscribedTopics.isEmpty()) {
            for (String topicId : subscribedTopics) {
                String topicName = topics.get(topicId);
                if (topicName != null) {
                    String publisher = topicPublishers.get(topicId).iterator().next();
                    out.println(topicId + " " + topicName + " " + publisher);
                }
            }
            out.println("\n");
        } else {
            out.println("exception No active subscriptions found\n");
        }

        System.out.println("Listed current subscriptions");
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        List<String> otherBrokers = new ArrayList<>();

        if (args.length > 1) {
            otherBrokers.addAll(Arrays.asList(Arrays.copyOfRange(args, 1, args.length)));
        }

        Broker broker = new Broker(port, otherBrokers);
        broker.start();
    }
}
