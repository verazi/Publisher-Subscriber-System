import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class Subscriber {
    private String brokerIp;
    private int brokerPort;
    private Set<String> subscribedTopics; // Track subscribed topics

    public Subscriber(String brokerIp, int brokerPort) {
        this.brokerIp = brokerIp;
        this.brokerPort = brokerPort;
        this.subscribedTopics = new HashSet<>(); // Initialize the subscription set
    }

    public void start() {
        try (Socket socket = new Socket(brokerIp, brokerPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            BufferedReader consoleInput = new BufferedReader(new InputStreamReader(System.in))) {

            System.out.println("Connected to Broker.\nPlease select command: list, sub, current, unsub");

            // Start a thread to listen for incoming messages from the broker
            new Thread(() -> {
                try {
                    StringBuilder messageBuffer = new StringBuilder();
                    String line;
                    while ((line = in.readLine()) != null) {
                        if (line.trim().isEmpty()) {
                            if (messageBuffer.length() > 0) {
                                System.out.println(messageBuffer.toString().trim());
                                messageBuffer.setLength(0);
                                System.out.print("Please select command: list, sub, current, unsub\n");
                            }
                        } else {
                            messageBuffer.append(line).append("\n");
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();

            // Main loop to handle user input
            String input;
            while (true) {
                input = consoleInput.readLine();
                String[] commands = input.split(" ", 2);
                String action = commands[0];

                switch (action) {
                    case "list":
                        out.println("list");
                        break;

                    case "sub":
                        if (commands.length < 2) {
                            System.out.println("Usage: sub {topic_id}");
                        } else {
                            String topicId = commands[1];
                            out.println("subscribe " + topicId);
                            subscribedTopics.add(topicId);
                        }
                        break;

                    case "current":
                        out.println("current");
                        break;

                    case "unsub":
                        if (commands.length < 2) {
                            System.out.println("Usage: unsub {topic_id}");
                        } else {
                            String topicId = commands[1];
                            out.println("unsubscribe " + topicId);
                            subscribedTopics.remove(topicId);
                        }
                        break;

                    default:
                        System.out.println("Invalid command. Available commands: list, sub {topic_id}, current, unsub {topic_id}");
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java Subscriber <username> <broker_ip> <broker_port>");
            return;
        }
        String brokerIp = args[1];
        int brokerPort = Integer.parseInt(args[2]);
        Subscriber subscriber = new Subscriber(brokerIp, brokerPort);
        subscriber.start();
    }
}
