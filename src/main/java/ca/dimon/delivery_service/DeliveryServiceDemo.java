package ca.dimon.delivery_service;

import ca.dimon.delivery_service.common.AeronMessagingConfiguration;
import java.util.ArrayList;
import java.util.List;
import ca.dimon.delivery_service.common.FunctionResult;
import ca.dimon.delivery_service.common.Aid;
import ca.dimon.delivery_service.message.Message;
import ca.dimon.delivery_service.message.MessageHeaderMimeType;
import ca.dimon.delivery_service.transport.TransportType;
import ca.dimon.delivery_service.transport.Transportable;
import ca.dimon.delivery_service.subscription.SubscriptionMatcher;
import java.util.Arrays;

public class DeliveryServiceDemo {

    private static String usage = "Usage:\n"
            + "java -jar target/delivery_service-0.1.0.jar client  /run/shm/aeron_messaging_server 192.168.12.34  44000 44001\n"
            + "java -jar target/delivery_service-0.1.0.jar server  /run/shm/aeron_messaging_client 0.0.0.0        44000 44001 55000 10";

    /**
     * <pre>
     *
     * This is our "main entry point" for the stand-alone demo. It would call
     * "demo.run()", which starts 5 clients connected to the delivery_service
     * and exchanging periodic messages.
     *
     * Usage:
     *   java -jar target/delivery_service-0.1.0.jar client  /run/shm/aeron_messaging_server 192.168.12.34  44000 44001
     *   java -jar target/delivery_service-0.1.0.jar server  /run/shm/aeron_messaging_client 0.0.0.0        44000 44001 55000 10
     *
     * </pre>
     *
     * @param args
     */
    public static void main(String[] args) {

        System.err.println("Debug: args (" + args.length + "): " + Arrays.toString(args));

        // TODO: improve args check, yet even better would be to add some library to have args "named" instead of relying on position.
        if (args.length != 5 && args.length != 7) {
            System.err.println("Error: expected 5 or 7 arguments, but got " + args.length + " instead. Arguments were: " + Arrays.toString(args) + "\n" + usage);
            return;
        }

        // Since main() is a static f-n, let's already create an instance of DeliveryServiceDemo and call run() on it.
        DeliveryServiceDemo demo = new DeliveryServiceDemo();

        // For the demo purpses let's have aeron enabled for both cases: client and server
        boolean enable_aeron = true;

        // args[0] - server|client
        String role_str = args[0];
        AeronMessagingConfiguration.ROLE role;
        if (role_str.equals("server")) {
            role = AeronMessagingConfiguration.ROLE.SERVER;
        } else if (role_str.equals("client")) {
            role = AeronMessagingConfiguration.ROLE.CLIENT;
        } else {
            System.err.println("Error: unsuppported role: " + role_str);
            return;
        }

        // args[1] - path to the directory to use (better memory mapped under "/run/shm/"
        String directory_str = args[1];

        // args[2] - ip address (in case of server: one to bind: either 0.0.0.0 to listen on public i-face avaliable to others or 127.0.0.1 to listen only on the local i-face
        String address_str = args[2];

        // args[3] - initial_data_port
        int initial_data_port = Integer.parseInt(args[3]);

        // args[4] - initial_control_port
        int initial_control_port = Integer.parseInt(args[4]); // aeron server "control port" (one for all newly connected clients)

        // args[5] - local_clients_base_port
        // args[6] - local_clients_base_port
        int local_clients_base_port = 0;
        int client_count = 0;
        if (args.length == 6) {
            local_clients_base_port = Integer.parseInt(args[5]); // client will use this port to open listening connection (aeron server -> aeron client)
            client_count = Integer.parseInt(args[6]);  // max number of allowed clients to be connected to the aeron server
        }

        // Create delivery service (this will create either server or client - depending on the passed arguments
        try {
            demo.create_delivery_service(
                    enable_aeron,
                    role,
                    directory_str,
                    address_str, // local server address (if create a server), or server's IP address to connect (if create client)
                    initial_data_port,
                    initial_control_port,
                    local_clients_base_port, // last 2 parameters are only for server configuration
                    client_count // only for server
            );
        } catch (Exception ex) {
            System.err.println("Exception cought: " + ex);
            return;
        }

        // Create and run several clients (either local or remote)
        if (role_str.equals("server")) {
            demo.run_clients_on_server_side();
        } else if (role_str.equals("client")) {
            demo.run_clients_on_remote_client_side();
        }
    }

    /**
     * T
     */
    class DeliveryServiceDemoRemoteClient extends Transportable {

        // Each client is different only by the "client_number" - simple integer value passed to it's constructor.
        int my_client_number;

        // Constructor takes client number (for our 5 clients client_nubmer will be: 0, 1, ... 4)
        public DeliveryServiceDemoRemoteClient(int client_number) {
            this.my_client_number = client_number;
            System.out.println("constructor: I'm client #" + client_number + " my uri: " + get_uri());
        }

        public void main() {
            // Let's simplify the client uris (instead of defalut unique strings like: "DeliveryServiceDemoClient@723279cf"
            // we will use "client_0", "client_1" etc. The whole purpose of instance uri is to be just unique string.
            this.set_uri("client_" + my_client_number);

            System.out.println("main(): I'm client #" + my_client_number + ", my uri is: " + get_uri());
            run_own_thread();
        }

        private void run_own_thread() {

        }
    }

    /**
     * We'll create a few clients, each client is an instance of the
     * DeliveryServiceDemoClient class. Each client has its own thread.
     *
     */
    class DeliveryServiceDemoClient extends Transportable {

        // Each client is different only by the "client_number" - simple integer value passed to it's constructor.
        int my_client_number;

        // Constructor takes client number (for our 5 clients client_nubmer will be: 0, 1, ... 4)
        public DeliveryServiceDemoClient(int client_number) {
            this.my_client_number = client_number;
            System.out.println("constructor: I'm client #" + client_number + " my uri: " + get_uri());
        }

        public void main() {
            // Let's simplify the client uris (instead of defalut unique strings like: "DeliveryServiceDemoClient@723279cf"
            // we will use "client_0", "client_1" etc. The whole purpose of instance uri is to be just unique string.
            this.set_uri("client_" + my_client_number);

            System.out.println("main(): I'm client #" + my_client_number + ", my uri is: " + get_uri());
            run_own_thread();
        }

        /**
         * The run_own_thread() is a "main endless loop", which is got executed
         * by each client. The run_own_thread(), as its name suggests, runs own
         * "per client" thread.
         */
        private void run_own_thread() {

            ///////////////////////////////////// Thread (begin) //////////////////////////
            //
            //
            Runnable runnable = () -> {

                // Just move 5 clients threads slightly apart in time, so the output is not overlapped mess.
                Aid.sleep_ms(my_client_number * 5);

                // Get myself a transport (this will also automagically subscribe us to 2 things: to "/broadcast" and to "my own uri")
                transport = delivery_service.create_new_transport(this, TransportType.LOCAL);

                // Flag indicates if we're subscribed or not
                boolean is_subscribed = false;

                // Start client_N's thread main endless loop
                long iteration_counter = 0;
                while (true) {
                    iteration_counter++;

                    // client_0 publishes some messages
                    if (my_client_number == 0) {
                        // Let's make 1st client to print it's iteration counter (even though all other clients have their own "main looop" and own "iteration counter" in parallel..
                        if (iteration_counter % 1000 == 0) {
                            System.out.println("========================= main loop iteration " + iteration_counter + " ================================");
                        }

                        // client_0 sends a "publish" message to "/scanners" every second
                        String from = get_uri();
                        String to = "/scanners";
                        String body = "client_0 sends a \"publish\" message to \"/scanners\" every second (send on iteration #" + iteration_counter + ")"; // Some body of the message... Body can be any java Object (String, Array etc.)";
                        Message message = Message.create_new_publish(from, to, body);
                        publish(message);

                        // client_0 sends request message to "client_1" on every 5th second (client_1 supposed to send back "response" message back to client_0)
                        if (iteration_counter % 5 == 0) {
                            // time to send client_0 -> client_1 request
                            from = get_uri();
                            to = "client_1";
                            body = "Private request from client_0 to client_1 (send on iteration #" + iteration_counter + ")";
                            Message request_message = Message.create_new_request(from, to, body);
                            send_request(request_message);
                        }

                        // client_0 sends broadcast message once when the iteration coutner is 7
                        if (iteration_counter % 11 == 0) {
                            // time to send client_0 -> client_1 request
                            from = get_uri();
                            to = "/broadcast";
                            body = "This is broadcast message from client_0 to all! (send on iteration #" + iteration_counter + ")";
                            Message request_message = Message.create_new_request(from, to, body);
                            FunctionResult send_request_result = send_request(request_message);
                            if (send_request_result.failed()) {
                                System.err.println("Error: failed to send_request(). Details: " + send_request_result.get_message());
                            }
                        }
                    }

                    // client_1 subscribes to "/scanners"
                    if (my_client_number == 1) {
                        // Check if subscribed
                        if (is_subscribed == false) {
                            // Not subscribed yet. Subscribe now by defining ISubscriptionMatcher lambda matcher f-n.
                            SubscriptionMatcher subscription_matcher = new SubscriptionMatcher(message -> message.headers.get("to").equals("/scanners"));
                            subscribe(subscription_matcher, "/scanners"); // Note: the 2nd argument is an optional "comment" / "description".
                            is_subscribed = true;
                        }
                    }

                    // all clients: Print 1-liner report on what's going on for this client
                    if (incoming_messages_queue.size() > 0) {
                        System.out.println(get_uri() + " my incoming_messages_queue size: " + incoming_messages_queue.size());

                        // all clients: Process all the messages form the "inbox"
                        while (incoming_messages_queue.size() > 0) {
                            Message incoming_message = incoming_messages_queue.remove(0);  // pop 1st message from the incoming_messages_queue
                            System.out.println(get_uri() + " processing message: " + incoming_message.to_json());
                            // if got request, must send back response:
                            if (incoming_message.headers.get("mime_type").equals(MessageHeaderMimeType.REQUEST)) {
                                // yes, got a request, sending response
                                String from = get_uri();
                                String body = "Response to your request, which was: " + incoming_message.body + " (send back on iteration #" + iteration_counter + ")";
                                Message response_message = Message.create_new_response(from, body, incoming_message);
                                send_response(response_message);
                            }
                        }
                    }

                    // Periodically print 2 subscription tables (one "common" and one "per request/response transactions")
                    if (my_client_number == number_of_clients_to_create_on_server_side - 1) {
                        System.out.println(delivery_service.visualize_subscriptions_common_table());
                        System.out.println(delivery_service.visualize_subscriptions_transactions_table());
                    }
                    // Sleep 1 second before the next loop
                    Aid.sleep_ms(1000);
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
            //
            ///////////////////////////////////// Thread (end) //////////////////////////
        }
    }

    DeliveryService delivery_service;
    List<DeliveryServiceDemoClient> demo_clients_on_server_side = new ArrayList<>();
    List<DeliveryServiceDemoRemoteClient> demo_clients_on_remote_client_side = new ArrayList<>();

    int number_of_clients_to_create_on_server_side = 5;
    int number_of_clients_to_create_on_remote_client_side = 3;

    public void create_delivery_service(
            boolean enable_aeron,
            AeronMessagingConfiguration.ROLE role,
            String directory_str,
            String address_str, // local server address (if create a server), or server's IP address to connect (if create client)
            int initial_data_port,
            int initial_control_port,
            int local_clients_base_port, // last 2 parameters are only needed for server configuration
            int client_count // only for server configuration
    ) throws Exception {
        System.out.println("Creating DeliveryService instance...");

        AeronMessagingConfiguration aeron_messaging_configuration = null;
        if (enable_aeron == true) {
            switch (role) {
                case CLIENT:
                    aeron_messaging_configuration = AeronMessagingConfiguration.create_aeron_client_configuration(
                            directory_str,
                            address_str,
                            initial_data_port,
                            initial_control_port);
                    break;
                case SERVER:
                    aeron_messaging_configuration = AeronMessagingConfiguration.create_aeron_server_configuration(
                            directory_str,
                            address_str,
                            initial_data_port,
                            initial_control_port,
                            local_clients_base_port,
                            client_count);
                    break;
                default:
                    System.err.println("Warning: while creating AeronMessagingConfiguration got unexpected role: " + role);
            }
        }

        // Create delivery service instance. The only argument it needs is all the aeron-related settings.
        delivery_service = DeliveryService.get_singleton_instance(aeron_messaging_configuration);
    }

    /**
     *
     */
    public void run_clients_on_server_side() {
        System.out.println("run_clients_on_server_side(): Creating " + number_of_clients_to_create_on_server_side + " client classes (each use it's own thread)...");

        for (int i = 0; i < number_of_clients_to_create_on_server_side; i++) {
            DeliveryServiceDemoClient ith_client = new DeliveryServiceDemoClient(i);
            demo_clients_on_server_side.add(ith_client);
            ith_client.main();
        }
    }

    /**
     *
     */
    public void run_clients_on_remote_client_side() {
        System.out.println("run_clients_on_remote_client_side(): Creating " + number_of_clients_to_create_on_remote_client_side + " client classes (each use it's own thread)...");

        for (int i = 0; i < number_of_clients_to_create_on_remote_client_side; i++) {
            DeliveryServiceDemoRemoteClient ith_client = new DeliveryServiceDemoRemoteClient(i);
            demo_clients_on_remote_client_side.add(ith_client);
            ith_client.main();
        }
    }

}
