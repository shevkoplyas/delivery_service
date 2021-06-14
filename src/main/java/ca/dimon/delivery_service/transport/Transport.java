package ca.dimon.delivery_service.transport;

import ca.dimon.delivery_service.DeliveryService;
import ca.dimon.delivery_service.common.FunctionResult;
import ca.dimon.delivery_service.message.Message;
import ca.dimon.delivery_service.subscription.SubscriberDetails;
import ca.dimon.delivery_service.common.ManagedObject;
import ca.dimon.delivery_service.subscription.SubscriptionMatcher;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ITransport is the common interface for all the transports. Let's also extend
 * ManagedObject, so each transport would have own stats and all the "increment"
 * and "get" methods associated with stats.
 */
public abstract class Transport extends ManagedObject {

    // Each transport instance always connects two points - delivery service and some "transport-able" instance that can send/receive messages.
    Transportable client;
    DeliveryService delivery_service;
    TransportType transport_type;

    // Each transport could just hold some stats counters re: nubmer of times this
    // particular transport instance is subscribed (by on of tree types of subscrption tables 
    // available in the delivery service (i.e. "commaon" , "transactional" and the "sniffers").
    // Instead of dry numbers let's simply have exact same records (as in actual subsdcription
    // bables) stored locally inside the given transport instance!
    // This would allow us to quickly see which susbciptions are using this transport!
    // For this magic to work we need to carefully add/remove records when transport subscribes
    // see DeliveryService.subscribe(), subscribe_transaction() and subscriptions_transactions_sniffers().
    //
    public final ConcurrentHashMap<SubscriptionMatcher, SubscriberDetails> subscriptions_common = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<SubscriptionMatcher, SubscriberDetails> subscriptions_transactions = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<SubscriptionMatcher, SubscriberDetails> subscriptions_transactions_sniffers = new ConcurrentHashMap<>();

    Transport(Transportable client, DeliveryService delivery_service, TransportType transport_type) {
        this.client = client;
        this.delivery_service = delivery_service;
        this.transport_type = transport_type;
    }

    /**
     * The "client" property has default access modifier (which is
     * package-private), so it won't be publicly available, but sometimes we
     * might wonder what is the client uri associated with this transport -
     * here's the "get_client_uri() method to answer this question (but it
     * mostly for debugging purposes).
     *
     * @return
     */
    public String get_client_uri() {
        return client.get_uri();
    }

    /**
     * General "deliver(message)" will figure out which type of the message is
     * given and it will use eiher: - deliver_publish - deliver_request -
     * deliver_response
     *
     * @param message
     */
    public abstract FunctionResult deliver(Message message);

    /**
     * Case: client -> delivery (publishes a message)
     *
     * @param message
     */
    public abstract FunctionResult deliver_publish(Message message);

    /**
     * Case: client -> delivery (client sends a request (which is "transactional
     * thing", i.e. has "transaction_id" message header and might get mulrople
     * replies from different sources))
     *
     * @param message
     */
    public abstract FunctionResult deliver_request(Message message);

    /**
     * Case: client -> delivery (client sends a response to previously received
     * request)
     *
     * @param message
     */
    public abstract FunctionResult deliver_response(Message message);

    /**
     * Case: delivery -> client (a message is about to be enqueued by transport
     * into the "client" to be futher processed (by client).
     *
     * @param message
     */
    public abstract FunctionResult deliver_back_to_client(Message message);

    /**
     * Case: client wants to subscribe to some "virtual channel" (example:
     * "/room/temperature" updates or "/scanners" or it can subscribe to some
     * other instance uri and then it will start receiving those messages along
     * with mentioned other instance.
     *
     * @param selector
     * @param expiration_epoch_ms
     */
    public abstract void subscribe(SubscriptionMatcher subscription_matcher);

    public abstract void subscribe(SubscriptionMatcher subscription_matcher, long expiration_epoch_ms);

    public abstract void subscribe(SubscriptionMatcher subscription_matcher, long expiration_epoch_ms, String subscriber_details_description);

    /**
     * Unsubscribe by the given SubscriptionMatcher instance.
     */
    public abstract void unsubscribe(SubscriptionMatcher subscription_matcher);
}
