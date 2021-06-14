package ca.dimon.delivery_service;

import ca.dimon.aeronmessaging.client.AeronMessagingClient;
import ca.dimon.aeronmessaging.client.AeronMessagingClientConfiguration;
import ca.dimon.delivery_service.message.Message;
import ca.dimon.delivery_service.message.MessageHeaderMimeType;
import ca.dimon.delivery_service.common.FunctionResult;
import ca.dimon.delivery_service.common.Aid;
import ca.dimon.delivery_service.subscription.SubscriberDetails;
import ca.dimon.delivery_service.transport.Transport;
import ca.dimon.delivery_service.transport.TransportType;
import ca.dimon.delivery_service.transport.Transportable;
import ca.dimon.delivery_service.transport.LoopbackTransport;
import ca.dimon.delivery_service.transport.LocalTransport;
import ca.dimon.delivery_service.transport.AeronTransport;
// --- these are from "aeronmessaging" dependency project ---(begin)-----------
import ca.dimon.aeronmessaging.common.IMessageHandler;
import ca.dimon.aeronmessaging.server.AeronMessagingServer;
import static ca.dimon.aeronmessaging.server.AeronMessagingServer.create;
import ca.dimon.aeronmessaging.server.AeronMessagingServerConfiguration;
import ca.dimon.aeronmessaging.server.ImmutableAeronMessagingServerConfiguration;
import ca.dimon.aeronmessaging.server.MainConsumerLoopVars;
import ca.dimon.delivery_service.common.AeronMessagingConfiguration;
// --- these are from "aeronmessaging" dependency project ---(end)-----------

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import ca.dimon.delivery_service.subscription.SubscriptionMatcher;
import java.time.Clock;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <pre>
 * The "Delivery Service" is the core piece gluing all the messaging between java
 * instances. All messaging participants have their own transports (instance derived from Transport class),
 * which connect them with the "Delivery Service".
 *
 * There sopposed to be only one instnce of the DeliveryService class (singleton), thus please use
 * static factory:
 *
 *     DeliveryService delivery_service = DeliveryService.get_singleton_instance();
 *
 * </pre>
 */
public class DeliveryService extends Transportable {

    // Common subscription table
    private final ConcurrentHashMap<SubscriptionMatcher, SubscriberDetails> subscriptions_common = new ConcurrentHashMap<>();

    // Transactions subscription table (while transaction sent and we're waiting for the response(s)
    // we'll route all the messages with mime_type: message/response back to the original requestor based
    // on this subscription table. Records should last only ut to "transaction timeout" time 
    // and then shold be cleared out (SubscriberDetails has expiration_epoch_ms after which transaction is expired).
    //
    // o(1) table for transaction route lookup table:
    //    - key = SubscriptionMatcher (it used to be just transaction_id (String), but matcher is better - it can keep state, like stats on the number of total found matches, total calls, timing, frequencies, etc. Also it would conform other "subscriptions_common" lookup table.
    //    - value = SubscriberDetails (stores corresponding transport instance)
    //
    private final ConcurrentHashMap<SubscriptionMatcher, SubscriberDetails> subscriptions_transactions = new ConcurrentHashMap<>();

    // The "subscriptions_transactions_sniffers" with a HashMap<String uri, SubscriberDetails> 
    // for all the instances who wants to also listen responses to other instances (like sniffers).
    // Only useful for some debugging and stats. Reason - we don't want to use both subscription
    // lookup tables: (1) subscriptions_transactions and (2) subscriptions_common to route the
    // response, instead normally we'd only look for the 1st match in 
    // (1) subscriptions_transactions and deliver response, but this logic would disallow us
    // to create a transportable class, which can see ALL types of meesges (with susbscription
    // matcher return true all the time) because response is only routed to the original sender.
    // So this new subscription table "subscriptions_transactions_sniffers" is and exception
    // (useful only for debugging, and educational purposes) and normally should be empty,
    private final ConcurrentHashMap<SubscriptionMatcher, SubscriberDetails> subscriptions_transactions_sniffers = new ConcurrentHashMap<>();

    // We store all of the existing transports "connected" to this delivery service here.
    //   - key = client instance uri (String)
    //   - value = reference to the transport instance associated with this client (strictly one transport per client)
    // 
    private final HashMap<String, Transport> transports = new HashMap<>();

    // Singleton pattern: we'll use static factory to instantiate the only DeliveryService instance.
    // For tutorial on that topic see this for article: https://www.baeldung.com/java-singleton
    // also "constructor vs static factories": https://www.baeldung.com/java-constructors-vs-static-factory-methods
    private static volatile DeliveryService instance = null;

    // Each time we route a message (see functions find_subscriptions_common() and find_subscriptions_transactions())
    // we periodically check subscription expiration and remove expired items. These 2 values allow us to avoid doing
    // expired records check on every message (note: we don't have dedicated "garbage collector" thread and instead 
    // we use "lazy-style cleanup" when we cleanup only "on occasion", so if no messages flying by, then stale
    // subscription records might sit in the tables for a loong long time..
    private long check_expired_subscriptions_common_min_interval_ms = 1000;
    private long check_expired_subscriptions_transactions_min_interval_ms = 1000;

    /**
     * Static factory to generate the DeliveryService instance.
     *
     * AeronMessagingConfiguration, as the only argument, tells delivery service
     * if we need to enable Aeron and if yes, then shall it be client or server
     * along with all the other corresponding options.
     *
     * @param aeron_messaging_configuration
     * @return
     */
    public static DeliveryService get_singleton_instance(AeronMessagingConfiguration aeron_messaging_configuration)
            throws Exception {

        if (instance == null) {
            synchronized (DeliveryService.class) {
                if (instance == null) {
                    // Create DeliverySerivce (singleton)
                    instance = new DeliveryService();

                    // Since the DeliveryService is the singleton let's give it concrete hardcoded "well-known" uri:
                    instance.set_uri("delivery_service");

                    // Create a special "loobback" transport - this transport is only needed by the DeliveryService, no other classes will ever need it.
                    instance.transport = instance.create_new_transport(instance, TransportType.LOOPBACK);

                    // Kick-start own message shovelling thread
                    instance.run_own_shovel_incoming_messages_thread();

                    // Start Aeron client or server (depending on the aeron_settings) only if the 'aeron_messaging_configuration' is not null
                    if (aeron_messaging_configuration != null) {
                        // Yes we should enable Aeron. Checking role:
                        switch (aeron_messaging_configuration.get_role()) {
                            case CLIENT:
                                // Strart Aeron client
                                instance.start_aeron_client(aeron_messaging_configuration.aeron_client_configuration);
                                break;
                            case SERVER:
                                // Start aeron_messaging_server in it's own thread, we'll use it's public
                                // methods to enqueue (send) / dequeue (receive) messages
                                instance.start_aeron_server(aeron_messaging_configuration.aeron_server_configuration);
                                break;
                            default:
                        }
                    }
                }
            }
        }
        return instance;
    }

    // ----------------------------------- AeronMessaging client/server (begin) -----------------
    private AeronMessagingServer aeron_messaging_server;
    private AeronMessagingClient aeron_messaging_client;

    /**
     * <pre>
     * Stop all the activity of the AeronMessaging clietn / server if any of those were created.
     * This includes:
     *     - stopping AeronMessagingServer's thread
     *     - closing Aeron
     *     - closing Aeron's media driver
     *
     * </pre>
     */
    private void stop_aeron() {
        if (aeron_messaging_server != null) {
            aeron_messaging_server.close();
        }
        if (aeron_messaging_client != null) {
            aeron_messaging_client.close();
        }
    }

    /**
     * <pre>
     * MessageHandler will be responsible for the incoming (via Aeron) messages processing.
     * We register an instance of this type with the aeron_messaging_server by the following call:
     *     aeron_messaging_server.register_external_message_handler(message_handler);
     * </pre>
     */
    private class MessageHandler implements IMessageHandler {

        @Override
        public void process_all_clients_message(String message) {
            System.out.println("process_all_clients_message: message: " + message);
        }

        @Override
        public void process_private_message(Integer session_id, String message) {
            System.out.println("process_private_message: session_id: " + session_id + ", message: " + message);
        }
    }

    private final MessageHandler message_handler = new MessageHandler();

    /**
     * Create AeronMessagingServer and start it in the separate thread
     * (aeron_messaging_server.aeron_messaging_server_thread).
     *
     * @throws Exception
     */
    private void start_aeron_server(AeronMessagingServerConfiguration aeron_server_configuration)
            throws Exception {

        // Start aeron_messaging_server in it's own thread, we'll use it's public methods to enqueue (send) / dequeue (receive) messages
        aeron_messaging_server = create(Clock.systemUTC(), aeron_server_configuration);
        aeron_messaging_server.register_external_message_handler(message_handler);
        aeron_messaging_server.aeron_messaging_server_thread = new Thread(aeron_messaging_server);
        aeron_messaging_server.aeron_messaging_server_thread.start();

        //
        // Main consumer loop (it uses aeron_messaging_server to send receive messages from the client(s))
        //
//        while (true) {
//            long received_messages_count = aeron_main_consumer_loop_iteration();
//            if (received_messages_count == 0) {
//              Thread.sleep(1);
//            }
//        }
    }

    private void start_aeron_client(AeronMessagingClientConfiguration aeron_client_configuration)
            throws Exception {

        // Start aeron_messaging_server in it's own thread, we'll use it's public methods to enqueue (send) / dequeue (receive) messages
        aeron_messaging_client = AeronMessagingClient.create(aeron_client_configuration);
        aeron_messaging_client.aeron_messaging_client_thread = new Thread(aeron_messaging_client);
        aeron_messaging_client.aeron_messaging_client_thread.start();

        //
        // Main consumer loop (it uses aeron_messaging_server to send receive messages from the client(s))
        //
//        while (true) {
//            long received_messages_count = aeron_main_consumer_loop_iteration();
//            if (received_messages_count == 0) {
//              Thread.sleep(1);
//            }
//        }
    }    
    //
    // ----------------------------------- AeronMessaging client/server (end) -----------------
    //
    /**
     * <pre>
     * Disabling constructor to force class user to use static factory function:
     * Example:
     *   DeliveryService delivery_service = DeliveryService.get_singleton_instance(aeron_settings);
     *
     * </pre>
     */
    private DeliveryService() {
    }

    // Delivery Service has it's own dequeue thread
    // to shovel the incoming_messages_queue
    private void run_own_shovel_incoming_messages_thread() {

        // Get myself a special "loobback" transport - this transport is only needed by the DeliveryService, no other classes will ever need it.
        this.transport = this.create_new_transport(this, TransportType.LOOPBACK);

        // Create a therad that will delete 1 element from myList every second
        Runnable runnable = () -> {

            // Start thread's main endless loop
            while (true) {

                // Quickly process all incoming messages (basically re-enqueue them in different destination by means of 
                // connected transports and subscription tables. Busy loop, no sleep, until we shovel them all.
                while (incoming_messages_queue.size() > 0) {
                    Message message = incoming_messages_queue.remove(0);
                    FunctionResult route_result = route(message);
                    if (route_result.failed()) {
                        // At least log error and increase stats
                        ; // don't report error / increase stats here, it was already done inside route() f-n.
                    }
                }

                // Sleep 1 ms before checking queue size again. (this would prevent 100% cpu
                // usage spikes at a price of adding "up to 1ms" delay into message processing.
                // TODO: figure out how to signal this thread to process incoming message 
                // as soon as it is placed in the queue, so we'll no need any sleep at all!-)
                Aid.sleep_ms(1);
            }
        };
        Thread thread = new Thread(runnable);
        thread.start();
    }

    /////////////////// family of subscribe_*() methods (begin) //////////////////////////
    //
    // There are 3 types of subscription: "common", "transactional" (request/response - subscription
    // by transaction_id) and one mostly for debug - "transaction sniffers".
    // Accordingly there are 3 functions to subscribe: regular "subscribe()",  "subscribe_transaction()"
    // when sending a request and lastly "subscribe_transactions_sniffers()" - only for debugging
    // (for participants who wants to listen to all "response" messages).
    //
    /**
     * Simple subscription (with no expiration, will last forever)
     *
     * @param subscription_matcher
     * @param transport
     * @param expire_on_epoch_ms
     */
    public void subscribe(SubscriptionMatcher subscription_matcher, SubscriberDetails subscriber_details) {
        // Add +1 item into subscriptions_common lookup table
        subscriptions_common.put(subscription_matcher, subscriber_details);

        // Also create +1 subscription record (duplicate) inside transport object (only per that particular transport)
        subscriber_details.transport.subscriptions_common.put(subscription_matcher, subscriber_details); // we duplicate subscription record inside transport just as a fancy way to transport to be aware about it's subscriptoins w/o lookup through all 3 delivery system subscription lookup tables.
    }

    /**
     * subscribe_transaction() will store transaction_id and corresponding
     * transport, so when "response" message is coming back we'll route it to
     * the original "request" sender by looking up the
     * "subscriptions_transactions" table. Note: we lookup the
     * "subscriptions_transactions" until 1st matching subscription is found, so
     * without introducing 3rd subscription table
     * "subscriptions_transactions_sniffers" it wouldn't be possible to
     * subscribe to all "response" messages when responses aren't sent "to:" our
     * instance (in other words without "subscriptions_transactions_sniffers"
     * there's no way to make a "catch-all sniffer").
     *
     * @param transaction_id
     * @param subscriber_details
     */
    public void subscribe_transaction(final String transaction_id, SubscriberDetails subscriber_details) {

        SubscriptionMatcher subscription_matcher = new SubscriptionMatcher(message -> message.headers.get("transaction_id").equals(transaction_id));

        // If by this time the subsciber_details does not have "description" property set,
        // lets put there transaction_id value (that field is like an optional comment,
        // only useful for debug-print subscription table itself)
        if (subscriber_details.description == null) {
            subscriber_details.description = transaction_id;
        }

        // Create +1 subscription record in "subscriptions_transactions" lookup table
        // and in similar-style "transport" instance (so transport is aware where
        // it is used/subscribed w/o additional lookups)
        subscriptions_transactions.put(subscription_matcher, subscriber_details);

        // Also create +1 subscription record (duplicate) inside transport object (only per that particular transport)
        subscriber_details.transport.subscriptions_transactions.put(subscription_matcher, subscriber_details); // we duplicate subscription record inside transport just as a fancy way to transport to be aware about it's subscriptoins w/o lookup through all 3 delivery system subscription lookup tables.
    }

    /**
     *
     * @param subscription_matcher
     * @param subscriber_details
     */
    public void subscribe_transactions_sniffers(SubscriptionMatcher subscription_matcher, SubscriberDetails subscriber_details) {
        // Add +1 item into subscriptions_transactions_sniffers lookup table
        subscriptions_transactions_sniffers.put(subscription_matcher, subscriber_details);

        // Add +1 item into subscriptions_transactions_sniffers inside transport (only holds "per this particula transport" subscriptions copies)
        subscriber_details.transport.subscriptions_transactions_sniffers.put(subscription_matcher, subscriber_details); // we duplicate subscription record inside transport just as a fancy way to transport to be aware about it's subscriptoins w/o lookup through all 3 delivery system subscription lookup tables.
    }

    /**
     * <pre>
     *  Automagically subscribe the caller to 3 things:
     * 1) to the "/broadcast" channel
     * 1) to the "/local/broadcast" channel
     * 2) to the caller "uri" string, so all participants can send requests/response from
     * one instnace to another (all instances are subscribed to their own uri).
     *
     * </pre>
     *
     * @param participant
     * @param participsnts_transport
     */
    private void subscribe_default_subscriptions(Transportable participant) {
        subscribe_default_subscriptions(participant, participant.transport);
    }

    /**
     * Sometimes the "participant" does not yet have transport assigned (example
     * we've just created a new transport via: create_new_transport() call) but
     * we need to add default subscriptions for that transport, then we use this
     * overloaded add_default_subscriptions() f-n with explicit Transport
     * argument.
     *
     * @param participant
     * @param participsnts_transport
     */
    private void subscribe_default_subscriptions(Transportable participant, Transport participsnts_transport) {

        // We could use only one SubscriberDetails, but let's make them all different instances due to the different comments we put inside "subscriber_details.description" field.
        {
            // Extra brackets would make declarations "local" and not visible in the next block, where we could accidentally missuse them.
            SubscriptionMatcher subscription_matcher = new SubscriptionMatcher(message -> message.headers.get("to").equals("/broadcast"));
            SubscriberDetails subscriber_details = new SubscriberDetails(participsnts_transport);
            subscriber_details.description = "/broadcast";
            subscribe(subscription_matcher, subscriber_details);
        }

        {
            SubscriptionMatcher subscription_matcher = new SubscriptionMatcher(message -> message.headers.get("to").equals("/local/broadcast"));
            SubscriberDetails subscriber_details = new SubscriberDetails(participsnts_transport);
            subscriber_details.description = "/local/broadcast";
            subscribe(subscription_matcher, subscriber_details);
        }

        {
            SubscriptionMatcher subscription_matcher = new SubscriptionMatcher(message -> message.headers.get("to").equals(participant.get_uri()));
            SubscriberDetails subscriber_details = new SubscriberDetails(participsnts_transport);
            subscriber_details.description = "by participant uri";
            subscribe(subscription_matcher, subscriber_details);
        }
    }

    //
    /////////////////// family of subscribe_*() methods (end) //////////////////////////
    /**
     * The unsubscribe() is only intended to serve the subscriptions_common
     * table. (for sniffers, which is "debug thing" we'll do somethign else if
     * ever)
     *
     * @param subscription_matcher
     */
    public void unsubscribe(SubscriptionMatcher subscription_matcher) {
        SubscriberDetails deleted_subscriber_details;
        deleted_subscriber_details = subscriptions_common.remove(subscription_matcher);

        deleted_subscriber_details.transport.subscriptions_common.remove(subscription_matcher); // we duplicated subscription record inside transport just as a fancy way to transport to be aware about it's subscriptoins w/o lookup through all 3 delivery system subscription lookup tables. So now it is time to delete it.
    }

    /**
     * DeliveryService (aka "router") transparently bypassing all the incoming
     * messages (from all kinds of connected to the delivery service transport
     * instances except "loopback transport") through itself by simply
     * forwarding incoming message to process to the route(message) function. If
     * we need to send the message to the delivery service itself it will be
     * routed to the special :
     *
     * @param message
     * @return
     */
    public FunctionResult process(Message message) {
        return route(message);
    }

    /**
     * </pre> This is special process function for the delivery service to
     * actually process incoming requests delivered via special
     * "LoobpackTransport" (used only by the delivery service) for cases when
     * request is addressed to the delivery serivice itself! The reqular
     * process(message) of the delivery service is simply routing all the
     * messages through (1 message in -> 1 message out) so the delivery service
     * become sort of "transparent", like a "transparent proxy" and thus it
     * would be excluded from the ability of all transportable instances to
     * receive the request addressed to the delivery service itself. To solve
     * this the "LoobpackTransport" was added and the corresponding subscription
     * table record, which associates the "delivery_service" uri with it's
     * "loobpack_transport", so all the messages addressed to the delivery
     * service will end up processed by the
     * process_message_from_loopback_transport(message) function!
     *
     * </pre>
     *
     * @param message
     */
    public void process_message_from_loopback_transport(Message message) {

    }

    /**
     * route(message) got called in a separate thread (owned by the
     * DeliveryService instance).
     *
     * @param message
     * @return
     */
    public FunctionResult route(Message message) {
        FunctionResult function_result = new FunctionResult();

        // Check if the message is expired, then simply drop it
        Long expiration_epoch_ms = (Long) message.headers.get("expiration_epoch_ms");
        if (expiration_epoch_ms != null) {
            // Message has "expiration_epoch_ms" header present
            if (expiration_epoch_ms != 0) {             // TODO: instead of blindly cast, make sure to check types and catch exceptions
                // Message has "expiration_epoch_ms" header non-zero value
                long now_epoch_ms = System.currentTimeMillis();
                if (expiration_epoch_ms < now_epoch_ms) {
                    // increment stats "route_dropped_messsages_count"
                    this.increment_stats("route_dropped_messsages_count");

                    // Expired, drop message (not an error)
                    function_result.succeed();
                    return function_result;
                }
            }
        }

        // Extract message -> header -> mime_type
        MessageHeaderMimeType mime_type = message.header_get_mime_type();
        if (mime_type == null) {
            // Report an error to logs and icrement stats error coutners (total and "per function name")
            String error_details = "failed to get message header mime_type";
            System.err.println(error_details);
            this.increment_stats("errors_count");         // this is our total errors counter
            this.increment_stats("route_errors_count");   // this is our "per function name" errors counter

            // Return failure
            function_result.set_fail(error_details);
            return function_result;
        }

        // Route the message depending on the message type (publish, request, response).
        List<SubscriberDetails> found_subscribers = null;
        switch (mime_type) {

            case PUBLISH:
            case REQUEST:
                // Find all matching subscriptions
                found_subscribers = find_subscriptions(message, subscriptions_common);

                // Iterate all found subscriber_details and send them a message
                for (SubscriberDetails subscriber_details : found_subscribers) {
                    // Send message to i-th subscriber
                    subscriber_details.transport.deliver_back_to_client(message);
                }
                break;

            case RESPONSE:
                // Find the only subscribed transport (by given transaction_id)
                SubscriberDetails found_subscriber = find_subscription(message, subscriptions_transactions);
                if (found_subscriber != null) {
                    // There can be either 1 or none records found, so it is "safe assumption"
                    found_subscriber.transport.deliver_back_to_client(message);
                } else {
                    // Failed to route the response. No subscribed transport found. But it is not necessarely an error.
                    // There can be several reasons:
                    //   - subscriber already de-registered (no longer exists)
                    //   - it took too long to come up with the response and transaction_id was already expired and cleared out from the subscription table
                }

                // Also do the lookup in 3rd subscription lookup table "subscriptions_transactions_sniffers"
                found_subscribers = find_subscriptions(message, subscriptions_transactions_sniffers);

                // Iterate all found subscriber_details and send them a message
                for (SubscriberDetails subscriber_details : found_subscribers) {
                    // Send message to the i-th subscriber
                    subscriber_details.transport.deliver_back_to_client(message);
                }
                break;

            default:
                // Note: stats will be increased and error reported by the caller f-n, so no hustle here..
                // error out`
                String error_details = "Error: failed to route(message): unknown message mime_type: " + message.mime_type;
                function_result.set_fail(error_details);
                return function_result;
        }

        // Return result: success
        function_result.set_success();
        return function_result;
    }

    /**
     * Visualize "subscriptions_common" lookup table as an ascii-table (String).
     *
     * @return
     */
    public String visualize_subscriptions_common_table() {
        StringBuilder result = new StringBuilder();
        Iterator it = subscriptions_common.entrySet().iterator();
        result.append("----------------------------------------------[ subscriptions_common ]-------------------------------------------\n");
        result.append(Aid.pad_string_with_spaces("           subscription matcher", 68)
                + " | " + Aid.pad_string_with_spaces(" transport.client_uri", 22)
                + " | " + Aid.pad_string_with_spaces(" subscription desc ", 19)
                + " | " + Aid.pad_string_with_spaces(" transport class", 18)
                + " | " + Aid.pad_string_with_spaces(" matches count", 15)
                + " | (transport registered #'s common/transactions/sniffers)\n");

        while (it.hasNext()) {
            // key = SubscriptionMatcher instance (basically matcher lambda)
            // value = SubscriberDetails (basically it's transport)
            Map.Entry pair = (Map.Entry) it.next();

            SubscriptionMatcher subscription_matcher = (SubscriptionMatcher) pair.getKey();
            String match_call_count_str = subscription_matcher.get_stats_value("match_call_count") != null ? " matched: " + subscription_matcher.get_stats_value("match_call_count").toString() : "";
            String match_found_count_str = subscription_matcher.get_stats_value("match_found_count") != null ? " matched: " + subscription_matcher.get_stats_value("match_found_count").toString() : "";

            SubscriberDetails subscriber_details = (SubscriberDetails) pair.getValue();
            String subscriber_details_description = subscriber_details.description != null ? subscriber_details.description : "-----";

            result.append(
                    Aid.pad_string_with_spaces(subscription_matcher.toString(), 68)
                    + " | " + Aid.pad_string_with_spaces(subscriber_details.transport.get_client_uri(), 22)
                    + " | " + Aid.pad_string_with_spaces(subscriber_details_description, 19)
                    + " | " + Aid.pad_string_with_spaces(subscriber_details.transport.getClass().getSimpleName(), 18)
                    + " | " + Aid.pad_string_with_spaces(match_found_count_str, 15)
                    + " | (" + subscriber_details.transport.subscriptions_common.size()
                    + " / " + subscriber_details.transport.subscriptions_transactions.size()
                    + " / " + subscriber_details.transport.subscriptions_transactions_sniffers.size()
                    + ")"
                    + "\n");
        }
        result.append("-----------------------------------------------------------------------------------------------\n");

        return result.toString();
    }

    /**
     * <pre>
     * Visualize "subscriptions_transactions" lookup table as an ascii-table
     * (String).
     *
     * subscriptions_transactions:
     *   - key = transaction_id (String)
     *   - value = SubscriberDetails (stores corresponding transport instance)
     *
     * </pre>
     *
     * @return
     */
    public String visualize_subscriptions_transactions_table() {
        StringBuilder result = new StringBuilder();
        Iterator it = subscriptions_transactions.entrySet().iterator();
        result.append("----------------------------------------------[ subscriptions_transactions ]-------------------------------------------\n");
        result.append(Aid.pad_string_with_spaces("           subscription matcher", 70)
                + " | " + Aid.pad_string_with_spaces(" transport.client_uri", 22)
                + " | " + Aid.pad_string_with_spaces(" subscription desc ", 52)
                + " | " + Aid.pad_string_with_spaces(" transport class", 18)
                + " | " + Aid.pad_string_with_spaces(" matches count", 15)
                + " | " + Aid.pad_string_with_spaces(" expire in ms", 15)
                + " | (subscr. counts common/transactions/sniffers)\n");

        while (it.hasNext()) {
            // key = SubscriptionMatcher instance (basically matcher lambda)
            // value = SubscriberDetails (basically it's transport)
            Map.Entry pair = (Map.Entry) it.next();

            SubscriptionMatcher subscription_matcher = (SubscriptionMatcher) pair.getKey();
            String match_call_count_str = subscription_matcher.get_stats_value("match_call_count") != null ? " matched: " + subscription_matcher.get_stats_value("match_call_count").toString() : "";
            String match_found_count_str = subscription_matcher.get_stats_value("match_found_count") != null ? " matched: " + subscription_matcher.get_stats_value("match_found_count").toString() : "";

            SubscriberDetails subscriber_details = (SubscriberDetails) pair.getValue();
            String subscriber_details_description = subscriber_details.description != null ? subscriber_details.description : "-----";

            Long time_till_subscription_expiration_ms = subscriber_details.time_till_subscription_expiration_ms();
            String expire_in_ms = time_till_subscription_expiration_ms != null ? time_till_subscription_expiration_ms.toString() : "n/a";

            result.append(
                    Aid.pad_string_with_spaces(subscription_matcher.toString(), 70)
                    + " | " + Aid.pad_string_with_spaces(subscriber_details.transport.get_client_uri(), 22)
                    + " | " + Aid.pad_string_with_spaces(subscriber_details_description, 52)
                    + " | " + Aid.pad_string_with_spaces(subscriber_details.transport.getClass().getSimpleName(), 18)
                    + " | " + Aid.pad_string_with_spaces(match_found_count_str, 15)
                    + " | " + Aid.pad_string_with_spaces(expire_in_ms, 15)
                    + " | (" + subscriber_details.transport.subscriptions_common.size()
                    + " / " + subscriber_details.transport.subscriptions_transactions.size()
                    + " / " + subscriber_details.transport.subscriptions_transactions_sniffers.size()
                    + ")"
                    + "\n");
        }
        result.append("-----------------------------------------------------------------------------------------------\n");

        return result.toString();
    }

    /**
     * <pre>
     * Timestamps table indicating when we last time cleared expired subscriptions from the
     * subscription lookup tables. This is used in the find_subscriptions() function.
     * HashMap has:
     *   - key = lookup_table_identity_hashcode
     *   - value = Long epoch_ms value indicating when last time we run cleanup on the given lookup table
     *
     * </pre>
     */
    private HashMap<String, Long> last_time_lookup_table_garbage_collection_timestamps = new HashMap<>(3);

    /**
     * Look through the whole subscriptions_common table and return all the
     * matching subscription details list.
     *
     * Also periodically do the "garbage collection" by removing expired
     * subscriptions. We don't have to do it on every message, but rather once
     * every 10-20 seconds should be fine.
     *
     * @param message
     * @return
     */
    public List<SubscriberDetails> find_subscriptions(Message message, ConcurrentHashMap<SubscriptionMatcher, SubscriberDetails> lookup_table) {
        boolean return_on_first_found = false;
        return find_subscriptions(message, lookup_table, return_on_first_found);
    }

    public List<SubscriberDetails> find_subscriptions(Message message, ConcurrentHashMap<SubscriptionMatcher, SubscriberDetails> lookup_table, boolean return_on_first_found) {

        // Function return the list of found subscribers
        List<SubscriberDetails> subscribers = new ArrayList<>();

        // Initialize 1st time seen lookup table "last time garbage collector called" timestamp with zero.
        String lookup_table_identity_hashcode = Aid.get_instance_identity_hashcode(lookup_table);
        if (last_time_lookup_table_garbage_collection_timestamps.get(lookup_table_identity_hashcode) == null) {
            last_time_lookup_table_garbage_collection_timestamps.put(lookup_table_identity_hashcode, 0L);
        }

        // Get the "last time garbage collector called" from the table
        Long last_time_lookup_table_garbage_collection_epoch_ms = last_time_lookup_table_garbage_collection_timestamps.get(lookup_table_identity_hashcode);

        // Figure out if it is a time to garbage-collect (delete expired records) subscription_common table.
        long now_epoch_ms = System.currentTimeMillis();
        boolean time_to_cleanup = now_epoch_ms - last_time_lookup_table_garbage_collection_epoch_ms > check_expired_subscriptions_common_min_interval_ms;
        if (time_to_cleanup) {
            // Update the "last time garbage collector called" value
            last_time_lookup_table_garbage_collection_timestamps.put(lookup_table_identity_hashcode, now_epoch_ms);
        }

        // Check if it is time to cleanup
        if (time_to_cleanup) {

            // OLD NICE-WAY to cleanup was these 5 lines.. new way (after we added "per-transport" 3 subscription tables inside each tranport is a FOR-loop below.. a bit more complex, but works file!
//                // Iterating over and removing from a map: https://stackoverflow.com/questions/1884889/iterating-over-and-removing-from-a-map
//                lookup_table.entrySet().removeIf(
//                        e
//                        -> e.getValue().expiration_epoch_ms != null
//                        && e.getValue().expiration_epoch_ms != 0
//                        && e.getValue().expiration_epoch_ms <= now_epoch_ms);
            for (Entry<SubscriptionMatcher, SubscriberDetails> entry : lookup_table.entrySet()) {
                SubscriptionMatcher subscription_matcher = entry.getKey();
                SubscriberDetails subscriber_details = entry.getValue();

                // Check expiration timestamp - is it set and is it in the past already?
                if (subscriber_details.expiration_epoch_ms != null
                        && subscriber_details.expiration_epoch_ms != 0
                        && subscriber_details.expiration_epoch_ms <= now_epoch_ms) {

                    // Delete found expired entry from the lookup_table
                    lookup_table.remove(subscription_matcher);

                    // Since we keep a copy of the subscription tables inside each transport
                    // (with all the records concerning that particular transport)
                    // we must remove given record from that particular transport as well:
                    //
                    // Lets figure out which 1 of 3 tables we're cleaning now (what was passed as "lookup_table"):
                    int number_of_removed_records = 0;
                    if (lookup_table == this.subscriptions_common) {
                        SubscriberDetails removed_subscriber_details = subscriber_details.transport.subscriptions_common.remove(subscription_matcher);
                        number_of_removed_records += removed_subscriber_details != null ? 1 : 0;

                    } else if (lookup_table == this.subscriptions_transactions) {
                        SubscriberDetails removed_subscriber_details = subscriber_details.transport.subscriptions_transactions.remove(subscription_matcher);
                        number_of_removed_records += removed_subscriber_details != null ? 1 : 0;
                    } else if (lookup_table == this.subscriptions_transactions_sniffers) {
                        SubscriberDetails removed_subscriber_details = subscriber_details.transport.subscriptions_transactions_sniffers.remove(subscription_matcher);
                        number_of_removed_records += removed_subscriber_details != null ? 1 : 0;
                    } else {
                        // Error: unknown type of the 'lookup_table' reference passes!
                        String error_details = "Error: while trying to cleanup expired subscripions got unknown lookup table reference.";
                        System.err.println(error_details);
                        this.increment_stats("errors_count");
                        this.increment_stats("find_subscriptions_errors_count");
                    }
                    // Check we actually found and removed exactly 1 record from 1 of 3 tables above (subscriber_details.transport.subscriptions_*)
                    if (number_of_removed_records != 1) {
                        // Error: find_subscriptions(): while removing expored records: failed check was: Check we actually found and removed exactly 1 record from 1 of 3 tables above (subscriber_details.transport.subscriptions_*)
                        String error_details = "Error: find_subscriptions(): while removing expored records: failed check was: Check we actually found and removed exactly 1 record from 1 of 3 tables above (subscriber_details.transport.subscriptions_*).";
                        System.err.println(error_details);
                        this.increment_stats("errors_count");
                        this.increment_stats("find_subscriptions_errors_count");
                    }
                }
            }
        }

        // Loop all items in the subscriptions_common table and find ones with
        // matching subscription (ones with subscription_matcher.match(message) returns true)
        for (SubscriptionMatcher subscription_matcher : lookup_table.keySet()) {
            if (subscription_matcher.match(message)) {
                // Collect SubscriberDetails (basically subscribed "transports")
                subscribers.add(lookup_table.get(subscription_matcher));
            }
        }

        // Return all found matchign subscribers
        return subscribers;
    }

    /**
     * The find_subscription() (singular) is a special case for calling
     * find_subscriptions() (plural) when we don't need to keep looking to the
     * end of the lookup table, but rather return 1st found element. Use case:
     * response delivery: we only need to fine the only subscribed transport in
     * the "subscriptions_transactions" table by given transaction_id.
     *
     * @param message
     * @param lookup_table
     * @return
     */
    public SubscriberDetails find_subscription(Message message, ConcurrentHashMap<SubscriptionMatcher, SubscriberDetails> lookup_table) {
        boolean return_on_first_found = true;
        List<SubscriberDetails> subscriptions = find_subscriptions(message, lookup_table, return_on_first_found);
        if (subscriptions.size() == 1) {
            return subscriptions.get(0);
        }

        // Nothing found
        return null;
    }

    /**
     * <pre>
     * Create a new transport instance for transportable instance, which wish to
     * participate in message delivery.
     * Note: this will also automagically subscribe caller to 2 things:
     *   1) to the "/broadcast" channel and to
     *   2) caller uri string, so all participants can send requests/response from
     *      one instnace to another (all instances are subscribed to their own uri).
     *
     * </pre>
     *
     * @param participant
     * @param transport_type
     * @return transport instance reference (if succeed) or null if failed.
     */
    public Transport create_new_transport(Transportable participant, TransportType transport_type) {
        // Check if this caller already got a transport instance, then simply return it (no need to produce warnings, but seems like poor decisions/design on the caller side)
        Transport existing_transport = this.transports.get(participant.get_uri());
        if (existing_transport != null) {
            return existing_transport;
        }

        // Create a new instance of the transport for the caller.
        Transport participsnts_transport = null;
        switch (transport_type) {
            case LOOPBACK:
                participsnts_transport = new LoopbackTransport(participant, this, TransportType.LOOPBACK);
                this.transports.put(participant.get_uri(), participsnts_transport);
                break;

            case LOCAL:
                participsnts_transport = new LocalTransport(participant, this, TransportType.LOCAL);
                this.transports.put(participant.get_uri(), participsnts_transport);
                break;

            case REMOTE_TUNNEL_AERON:
                participsnts_transport = new AeronTransport(participant, this, TransportType.REMOTE_TUNNEL_AERON);
                this.transports.put(participant.get_uri(), participsnts_transport);
                break;

            default:
                // Error: unsupported transort type
                // Report an error to logs and icrement stats error coutners (total and "per function name")
                String error_details = "Error: Failed to create_new_transport. Unsupported transport type: " + transport_type.toString();
                System.err.println(error_details);
                this.increment_stats("errors_count");
                this.increment_stats("create_new_transport_errors_count");
                break;
        }

        // Add default subscriptions for the new transport
        subscribe_default_subscriptions(participant, participsnts_transport);

        // Return caller the newly created transport
        return participsnts_transport;
    }
}
