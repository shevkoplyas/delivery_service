package ca.dimon.delivery_service.transport;

import ca.dimon.delivery_service.DeliveryService;
import ca.dimon.delivery_service.common.FunctionResult;
import ca.dimon.delivery_service.message.Message;
import ca.dimon.delivery_service.message.MessageHeaderMimeType;
import ca.dimon.delivery_service.subscription.SubscriberDetails;
import java.util.Objects;
import java.util.UUID;
import ca.dimon.delivery_service.subscription.SubscriptionMatcher;

/**
 * The "local" means this transport connects locally reachable instances to the
 * delivery service. We don't need to even serialize things and can simply pass
 * reference around since they all part of this current process. For contrast
 * there is AeronTransport (which has transportType == REMOTE_TUNNEL_AERON) and
 * that one is used to connect some remote systems which we only can talk to via
 * "serial port" (via UDP datagrams sent over internet by Aeron client/server).
 *
 */
public class LocalTransport extends Transport {

    public LocalTransport(Transportable client, DeliveryService delivery_service, TransportType transport_type) {
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
            message.headers.put("mime_type", MessageHeaderMimeType.PUBLISH);
        }
        // Enqueue the message into the corresponding queue
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

        // The "transaction_expiration_epoch_ms" meassage header must be set.
        // Take it's value from the message headersfor subscription expiration.
        Long transaction_expiration_epoch_ms = null;
        if (message.headers.get("transaction_expiration_epoch_ms") instanceof Long) {
            // Found "transaction_expiration_epoch_ms" in the message headers. Take it's value.
            transaction_expiration_epoch_ms = (Long) message.headers.get("transaction_expiration_epoch_ms");
        } else {
            // Error: the message headers does not specify "transaction_expiration_epoch_ms"
            String error_details = "Error: the message headers does not specify \"transaction_expiration_epoch_ms\". Details: message: " + message.to_json();
            result.set_fail(error_details);
            return result;
        }

        // Create the "subacriber details". For a simple LocalTransport we only need to put a reference to the transport
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
            message.headers.put("mime_type", MessageHeaderMimeType.RESPONSE);
        }

        // Enqueue the message into the corresponding queue
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

        // No need to modify message header mime_type.. it was presumably already set by the sender.
        // Enqueue the message into the corresponding queue
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
    public void subscribe(SubscriptionMatcher subscription_matcher, long expiration_epoch_ms, String optional_comments) {
        SubscriberDetails subscriber_details = new SubscriberDetails(this, expiration_epoch_ms);
        if (optional_comments != null) {
            subscriber_details.description = optional_comments;
        }
        delivery_service.subscribe(subscription_matcher, subscriber_details);
        increment_stats("subscribe_count");
    }

    @Override
    public void subscribe(SubscriptionMatcher subscription_matcher, long expiration_epoch_ms) {
        subscribe(subscription_matcher, expiration_epoch_ms, null);
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
