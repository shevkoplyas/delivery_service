package ca.dimon.delivery_service.subscription;

import ca.dimon.delivery_service.message.Message;

/**
 * The IMatcher interfaces is necessary for using lambda expression in a class
 * SubscriptionMatcher method "match(message)". This single-method interface is
 * used as a type of the parameter passed to the class SubscriptionMatcher
 * constructor, so we can preserve given lambda and later call it (see
 * SubscriptionMatcher.match(message) method).
 *
 * To use a lambda expression in a method, the method should have a parameter
 * with a single-method interface as its type. Calling the interface's method
 * will run the lambda expression.
 */
public interface IMatcher {

    Boolean match(Message message);
}
