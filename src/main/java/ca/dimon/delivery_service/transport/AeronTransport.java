package ca.dimon.delivery_service.transport;

import ca.dimon.delivery_service.DeliveryService;
import ca.dimon.delivery_service.common.FunctionResult;
import ca.dimon.delivery_service.message.Message;
import ca.dimon.delivery_service.message.MessageHeaderMimeType;
import ca.dimon.delivery_service.subscription.SubscriberDetails;
import java.util.Objects;
import java.util.UUID;
import ca.dimon.delivery_service.subscription.SubscriptionMatcher;

// TODO:
//Add default uri value to all the speculant classes based on the same logic (of getting clash hashmap Foo@223423423)
//   which shold be above public class ActiveRequest (it has uri and get_uri(), so it should be not only for Active requests, but for wider scope)
//             should add ManagedObject, which will have 'uri', 'get_uri()', 'mime_type' and may be stats and corresponding stats-methods?
//           config - hashmap String key / value (Object, so basically 'any')
//           stats -  hashmap String key / value (type? Object? we'll use Long, Ingeger, String and other classes')
//           get_config()
//           get_config_as_hash()
//           incremetn_stats_value(key, increment = 1)
//           get_stats()
//           get_stats_as_hash()
//           set_stats(hash_or_name, value = null)
//           set_stats_smart(hash_or_name, value = null)
//           
//           as_json()
//           to_json()
//           on_error(error_message) -> stats + error counter
//           change_state_to()  <--- interesting, where do I have state tracker? shouldn't I move it up to this level?'
//           
//Work on the AeronTransport (this may be postponed till after 2 simple example clients can talk to each other (or 10 clients randomly talking to eachother)).
//Work on /broadcast.
//Work on /local/broadcast.
//Who/how helps us to create a message with headers, body, and body with mime_type?
//Add stats into very foundation, so all classes have stats (what about "config" - the speculant already has those in place for "some" classes? think of that design).
//When do we serialze things and when we can simply pass them as an object? (no need to serialize for local transports! it is ok to have 2 "process(message)" and process(Object)).
//Work on "expire transactional subscription table"
//Read on "multiple inheritance in java" - how do we add "extends Transportable" for some classes easily?
/**
 *
 */
public class AeronTransport extends Transport {

    public AeronTransport(Transportable client, DeliveryService delivery_service, TransportType transport_type) {
        super(client, delivery_service, transport_type);
    }

    /**
     * General "deliver(message)" will figure out which type of the message is
     * given and it will use eiher: - deliver_publish - deliver_request -
     * deliver_response
     *
     * @param message
     */
    @Override
    public FunctionResult deliver(Message message) {
        FunctionResult result = new FunctionResult();

        switch (message.header_get_mime_type()) {
            case PUBLISH:
                this.deliver_publish(message);
                result.set_success();
                break;
            case REQUEST:
                this.deliver_request(message);
                result.set_success();
                break;
            case RESPONSE:
                this.deliver_response(message);
                result.set_success();
                break;
            default:
                String error_details = "Error: unknonwn message header mime_type '" + message.header_get_mime_type() + "'.";
                result.set_fail(error_details);
                increment_stats("errors_count");
                increment_stats("deliver_errors_count");
        }

        return result;
    }

    /**
     * Simple sending of the "publish" message - we enqueue it to the
     * delivery_service "inbox" and it will route() it for us.
     *
     * @param message
     */
    @Override
    public FunctionResult deliver_publish(Message message) {
        FunctionResult result = new FunctionResult();

        // Check inputs: we have a message with headers
        Objects.requireNonNull(message, "message");
        delivery_service.enqueue(message);
        increment_stats("deliver_publish_count");

        return result.set_success();
    }

    /**
     * <pre>
     * We're sending request from client to some other client via delivery service.
     * Two things need to happen:
     *   1) we add a record into delivery service transactional subscriptions so
     *      all the replies (with mime_type: "message_header/response") would be
     *      routed back to the OP (original poster:)
     *   2) "send" the message by enqueueing it into delivery service.
     *
     * </pre>
     *
     * @param message
     */
    @Override
    public FunctionResult deliver_request(Message message) {
        FunctionResult result = new FunctionResult();

        // Check inputs: we have a message with headers
        if (message == null) {
            String error_details = "deliver_reqeust() got null message reference.";
            return result.set_fail(error_details);
        }

        // Check inputs: we have a message with headers
        if (message.headers == null) {
            String error_details = "deliver_reqeust() got message with null headers reference.";
            return result.set_fail(error_details);
        }

        // Set proper message header mime_type if it isn't yet set
        if (message.headers.get("mime_type") == null) {
            // This mime_type supposed to be set by Message.create_new_request() static factory, but
            // if user build message himself (and forgot to put proper mime_type) let's fix it here by adding proper mime_type.
            message.headers.put("mime_type", MessageHeaderMimeType.REQUEST);
        }

        // Add unuque transaction id into the headers if message doesn't have such header set yet
        if (message.headers.get("transaction_id") == null) {
            // Generate new unique transaction_id message header
            UUID uuid = UUID.randomUUID();
            String new_transaction_id = "transactioon-id-" + uuid.toString();
            message.headers.put("transaction_id", new_transaction_id);
        }

        // The "transaction_expiration_epoch_ms" meassage header must be set, take it's value for subscription expiration.
        Long transaction_expiration_epoch_ms = null;
        if (message.headers.get("transaction_expiration_epoch_ms") instanceof Long) {
            // Error: the message headers does not specify "transaction_expiration_epoch_ms"
            String error_details = "Error: the message headers does not specify \"transaction_expiration_epoch_ms\". Details: message: " + message.to_json();
            result.set_fail(mime_type);
            return result;
        } else {
            // Found "transaction_expiration_epoch_ms" in the message headers. Take it's value.
            transaction_expiration_epoch_ms = (Long) message.headers.get("transaction_expiration_epoch_ms");
        }

        // Compose the "subacriber details". For a simple LocalTransport we only need to put a reference to the transport
        // instance itself into subscription table (no need for "tunnel headers" like for AeronTransport)
        SubscriberDetails subscriber_details = new SubscriberDetails(this, transaction_expiration_epoch_ms);

        // 1) we add a record into delivery service transactional subscriptions
        delivery_service.subscribe_transaction(message.headers.get("transaction_id").toString(), subscriber_details);

        // 2) "send" the message by enqueueing it into delivery service
        delivery_service.enqueue(message);
        increment_stats("deliver_request_count");

        return result.set_success();
    }

    /**
     * Client instance have processed the request and now is sending the
     * response (with mime_type: "message_header/response") back to the
     * "header.from" instance via delivery. Simply enqueue the message into the
     * delivery service and it will route() it accordingly (it will use
     * "subscriptions_transactions" lookup table (not the
     * "subscriptions_common", which is used to route "send publish" and "send
     * request" cases).
     *
     * @param message
     */
    @Override
    public FunctionResult deliver_response(Message message) {
        FunctionResult result = new FunctionResult();

        // Check inputs: we have a message
        Objects.requireNonNull(message, "message");
        delivery_service.enqueue(message);
        increment_stats("deliver_response_count");

        return result.set_success();
    }

    /**
     * Now response is going from service_delivery back to the requestor
     * ("client"). Simply enqueue(messsage) to the client "inbox".
     *
     * @param message
     */
    @Override
    public FunctionResult deliver_back_to_client(Message message) {
        FunctionResult result = new FunctionResult();

        // Check inputs: we have a message
        Objects.requireNonNull(message, "message");
        client.enqueue(message);
        increment_stats("deliver_back_to_client_count");

        return result.set_success();
    }

    /**
     * We'd never use loopback_transport.subscribe(...) since it makes no sense.
     * This method is here just because we need to implement all the abstract
     * methods of the "Transport" class, which we extend here.
     *
     * @param selector
     * @param expiration_epoch_ms
     */
    @Override
    public void subscribe(SubscriptionMatcher subscription_matcher, long expiration_epoch_ms, String subscriber_details_description) {
//        SubscriberDetails subscriber_details = new SubscriberDetails(this, expiration_epoch_ms);
//        if (subscriber_details_description != null) {
//            subscriber_details.subscriber_details_description = subscriber_details_description;
//        }
//        delivery_service.subscribe(subscription_matcher, subscriber_details);
//        increment_stats("subscribe_count");
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void subscribe(SubscriptionMatcher subscription_matcher, long expiration_epoch_ms) {
        increment_stats("subscribe_count");
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void subscribe(SubscriptionMatcher subscription_matcher) {
        subscribe(subscription_matcher, 0);
    }

    @Override
    public void unsubscribe(SubscriptionMatcher subscription_matcher) {
        delivery_service.unsubscribe(subscription_matcher);
        increment_stats("unsubscribe_count");
    }
}
