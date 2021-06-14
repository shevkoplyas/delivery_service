package ca.dimon.delivery_service.subscription;

import ca.dimon.delivery_service.message.Message;
import ca.dimon.delivery_service.common.ManagedObject;

/**
 * <pre>
 * This class allows us to pass a lambda function, which will be stored in the created instance
 * and later we'll be able to call stored lambda as "match(message)" instance method.
 * The "match(messsage)" function is a binary function, which returns true if given
 * criteria (expressed by caller inside lambda function) match some of the message fields.
 *
 * One usage example would be to simply "return true;", then
 * all the messages will be delivered to the subscriber.
 * Other example would be to: return message.to == "/scanners"; in which case all the
 * messages sent to "/scanners" virtual channel will be delivered to you.
 *
 * See: https://stackoverflow.com/questions/13604703/how-do-i-define-a-method-which-takes-a-lambda-as-a-parameter-in-java-8
 * in particular: https://stackoverflow.com/a/13604748/7022062
 *
 * </pre>
 */
public class SubscriptionMatcher extends ManagedObject {

    // Lambda (passed via constructor) will be stored here:
    private IMatcher lambda_matcher_function;

    /**
     * Constructor accepts lambda, which takes argument and return some value as
     * defined in IMatcher
     *
     * @param lambda_matcher_function
     */
    public SubscriptionMatcher(IMatcher lambda_matcher_function) {
        // Simply store given lambda.
        this.lambda_matcher_function = lambda_matcher_function;
    }

    /**
     * Call previously stored lambda, pass argument, return whatever lambda
     * returns.
     */
    public Boolean match(Message message) {
        // Run matcher lambda
        Boolean is_match = lambda_matcher_function.match(message);
        
        // Increment some SubscriptionMatcher stats counters
        this.increment_stats("match_call_count");
        if (is_match) {
            // If match found, increase stats conter
            this.increment_stats("match_found_count");
        }
        
        return is_match;
    }
}
