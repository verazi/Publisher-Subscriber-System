import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Publisher {
    private String publisherName;
    private String brokerIp;
    private int brokerPort;

    public Publisher(String publisherName, String brokerIp, int brokerPort) {
        this.publisherName = publisherName;
        this.brokerIp = brokerIp;
        this.brokerPort = brokerPort;
    }

    public void start() {
        try (Socket socket = new Socket(brokerIp, brokerPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            BufferedReader consoleInput = new BufferedReader(new InputStreamReader(System.in))) {

            System.out.println("Connected to Broker.");

            String input;
            while (true) {
                System.out.print("Please select command: create, publish, show, delete \n");
                input = consoleInput.readLine();
                String[] commands = input.split(" ", 2);
                String action = commands[0];

                switch (action) {
                    case "create":
                        if (commands.length < 2) {
                            System.out.println("Usage: create {topic_id} {topic_name}");
                        } else {
                            String[] createParts = commands[1].split(" ", 2);
                            if (createParts.length < 2) {
                                System.out.println("Usage: create {topic_id} {topic_name}");
                            } else {
                                String topicId = createParts[0];
                                String topicName = createParts[1];
                                out.println("create " + topicId + " " + topicName + " " + publisherName);
                                System.out.println(in.readLine());
                            }
                        }
                        break;

                        case "publish":
                        if (commands.length < 2) {
                            System.out.println("Usage: publish {topic_id} {message}");
                        } else {
                            String[] publishParts = commands[1].split(" ", 2);
                            if (publishParts.length < 2) {
                                System.out.println("Usage: publish {topic_id} {message}");
                            } else {
                                String topicId = publishParts[0];
                                String message = publishParts[1].trim();
                                if (message.length() > 100) {
                                    System.out.println("Message is too long. Max 100 characters.");
                                } else {
                                    out.println("publish " + topicId + " " + publisherName + " " + message);
                                    System.out.println(in.readLine());
                                }
                            }
                        }
                        break;
                        
                    case "show":
                        if (commands.length == 1) {
                            out.println("showAll " + publisherName);
                            String response;
                            while (!(response = in.readLine()).equals("END_OF_RESPONSE")) {
                                System.out.println(response);
                            }
                        } else {
                            String topicId = commands[1];
                            out.println("show " + topicId + " " + publisherName);
                            System.out.println(in.readLine());
                        }
                        break;

                    case "delete":
                        if (commands.length < 2) {
                            System.out.println("Usage: delete {topic_id}");
                        } else {
                            String deleteTopicId = commands[1];
                            out.println("delete " + deleteTopicId + " " + publisherName);
                            System.out.println(in.readLine());
                        }
                        break;

                    default:
                        System.out.println("Invalid command.");
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java Publisher <username> <broker_ip> <broker_port>");
            return;
        }
        String publisherName = args[0];
        String brokerIp = args[1];
        int brokerPort = Integer.parseInt(args[2]);
        Publisher publisher = new Publisher(publisherName, brokerIp, brokerPort);
        publisher.start();
    }
}
