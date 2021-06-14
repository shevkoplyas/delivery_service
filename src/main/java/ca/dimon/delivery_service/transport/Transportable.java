package ca.dimon.delivery_service.transport;

import ca.dimon.delivery_service.common.Aid;
import ca.dimon.delivery_service.common.FunctionResult;
import ca.dimon.delivery_service.message.Message;
import ca.dimon.delivery_service.common.ManagedObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import ca.dimon.delivery_service.subscription.SubscriptionMatcher;

/**
 * "Transport-Able", something that has a transport and cen use delivery
 * (router) to send/receive messages.
 *
 */
public class Transportable extends ManagedObject {

    public Transport transport;

    /**
     * <pre>
     * We could use ConcurrentLinkedDeque instead of synchronized list, but it
     * does not like .size() method (will have to iterate whole collection on
     * that). The only disadvantage of the synchronizedList is that it makes
     * "atomic" calls on that collection thread-safe, but if you create
     * iterator and about to do the loop to iterate all the items, you don't
     * want any other thread to modify (example: inject new item) your
     * collection thus you'd have to wrap whole "take iteration and loop them
     * all" into  a single synchronized block like this:
     *
     *     synchronize(incoming_messages_queue){ // create the incoming_messages_queue iterator and loop over that collection inside this synchronized block
     * </pre>
     */
    protected final List<Message> incoming_messages_queue = Collections.synchronizedList(new ArrayList<>());

    /**
     * Delivery Service will use our transport to send us messages (be it
     * responses or published messages, which match our subscription). All those
     * "incoming" messages will be delivered to us vie this "enqueue(message)"
     * method.
     *
     */
    public void enqueue(Message message) {
        incoming_messages_queue.add(message);
    }

    /**
     * <pre>
     * There are 3 ways to send a message:
     *   - publish(message)
     *   - send_request(request_message)
     *   - send_response(response_message)
     * 
     * </pre>
     */
    public FunctionResult publish(Message message) {
        return transport.deliver_publish(message);
    }

    public FunctionResult send_request(Message request_message) {
        return transport.deliver_request(request_message);
    }

    public FunctionResult send_response(Message response_message) {
        return transport.deliver_response(response_message);
    }

    /**
     * Subscribe function will create +1 record in the subscription table inside
     * the delivery service, all "publish" and "request" messages will be
     * matched against all the records in the subscription table and will be
     * delivered to the subscribed classes (via their transports) if
     * ISubscriptionMatcher function return true.
     *
     */
    public void subscribe(SubscriptionMatcher subscription_matcher) {
        transport.subscribe(subscription_matcher);
    }
    public void subscribe(SubscriptionMatcher subscription_matcher, String optional_comment) {
        transport.subscribe(subscription_matcher, 0, optional_comment);
    }
    
    /**
     * If you ever plan to unsubscribe, then preserve the SubscriptionMatcher you
     * used for the subscribe() call and use it again for unsubscribe() call later.
     * 
     * @param subscription_matcher 
     */
    public void unsubscribe(SubscriptionMatcher subscription_matcher) {
        transport.unsubscribe(subscription_matcher);
    }
}
