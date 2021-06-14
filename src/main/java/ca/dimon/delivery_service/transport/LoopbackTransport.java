package ca.dimon.delivery_service.transport;

import ca.dimon.delivery_service.DeliveryService;
import ca.dimon.delivery_service.common.FunctionResult;
import ca.dimon.delivery_service.message.Message;
import ca.dimon.delivery_service.subscription.SubscriberDetails;
import ca.dimon.delivery_service.subscription.SubscriptionMatcher;

/**
 * The better name for this class would be "DeliveryServiceLoopbackTransport"
 * since it is used only for the delivery servcie and only once and only to
 * connect it to "self", but seems more readable (and less-to-type name chosen
 * to be LoopbackTransport).
 *
 */
public class LoopbackTransport extends Transport {

    public LoopbackTransport(Transportable client, DeliveryService delivery_service, TransportType transport_type) {
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

    @Override
    public FunctionResult deliver_publish(Message message) {
        FunctionResult result = new FunctionResult();

        delivery_service.process_message_from_loopback_transport(message);
        increment_stats("deliver_publish_count");

        return result.set_success();
    }

    @Override
    public FunctionResult deliver_request(Message message) {
        FunctionResult result = new FunctionResult();

        delivery_service.process_message_from_loopback_transport(message);
        increment_stats("deliver_request_count");

        return result.set_success();
    }

    @Override
    public FunctionResult deliver_response(Message message) {
        FunctionResult result = new FunctionResult();

        delivery_service.process_message_from_loopback_transport(message);
        increment_stats("deliver_response_count");

        return result.set_success();
    }

    @Override
    public FunctionResult deliver_back_to_client(Message message) {
        FunctionResult result = new FunctionResult();

        delivery_service.process_message_from_loopback_transport(message);
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
        SubscriberDetails subscriber_details = new SubscriberDetails(this, expiration_epoch_ms);
        if (subscriber_details_description != null) {
            subscriber_details.description = subscriber_details_description;
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
