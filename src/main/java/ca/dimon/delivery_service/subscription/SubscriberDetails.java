package ca.dimon.delivery_service.subscription;

import ca.dimon.delivery_service.transport.Transport;
import java.util.HashMap;
import ca.dimon.delivery_service.subscription.SubscriptionMatcher;

/**
 * <pre>
 *
 * All the records in the DeliveryServcie subscription table will represent 2 items:
 *   1) matcher
 *   2) subscriber_details", where the "subscriber_details" will hold 2 properties:
 *       - transport reference
 *       - tunnel_headers (HashMap<String, String> (key = header name, value - corresponding header value).
 *
 * The added tunnel_headers will be null for all local transports, but for the aeron_transport
 * the tunnel_headers will hold the values:
 *   - subscriber_aeron_session_id
 *   - subscriber_transport_uri.
 *
 * In future, if we happen to have any other "is_tunnel = true" transports (like "FileTransport or HTTPTransport),
 * the "tunnel_headers" will have different transport-specific information which will help us to route the message
 * properly via the corresponding tunnel.
 *
 * <pre>
 *
 */
public class SubscriberDetails {

    public Transport transport;

    // tunnel_headers can be:
    //    - subscriber_transport_uri
    //    - subscriber_aeron_session_id
    public HashMap<String, String> tunnel_headers;

    // The "expireation_epoch_ms" header is used only for request/responses.
    // The idea is that after request it sent the original sender might get many
    // responses to it's request, but delivery system will only route resonses
    // until expiration_epoch_ms time and then will simply start dropping any
    // attempt to send expired transaction message(s).
    // Set value to null or to zero to indicate "no expiration" subscriptions.
    public Long expiration_epoch_ms;

    public SubscriptionMatcher subscription_matcher;
    public String description = null; // only useful for debug and/or educational purposes.. if set it might be used to tell (otherwise looking very similarly) subscriptions apart :)

    /**
     * 1/3 Simplified constructor used when our transport is a simple
     * "LocalTransport" and we don't need to deal with "tunnel headers" (like
     * we'd need for AeronTransport). It has no expiration (subscription will
     * last forever).
     *
     * @param transport
     */
    public SubscriberDetails(
            Transport transport
    ) {
        this(transport, null, null);
    }

    /**
     * 2/3 Next constructor has transport and expiration. This is useful for
     * example for local request sent with 20 seconds transaction expiration.
     *
     * @param transport
     * @param expiration_epoch_ms
     */
    public SubscriberDetails(
            Transport transport,
            Long expiration_epoch_ms
    ) {
        this(transport, expiration_epoch_ms, null);
    }

    /**
     * 3/3 Last constructor is only needed to serve tunnel-transports for remote
     * clients, like for AeronTransport, when the only transport connects
     * delivery service with aeron server and all the remote subscribers are
     * located somewhere far far away (all communication goes via serialized UDP
     * connectoin to the Aeron Client). In this case we'll add "aeron_session"
     * and "subscriber_transport_uri" into the message header, which are
     * represented here by the "tunnel_headers".
     *
     * @param transport
     * @param expiration_epoch_ms
     * @param tunnel_headers
     */
    public SubscriberDetails(
            Transport transport,
            Long expiration_epoch_ms,
            HashMap<String, String> tunnel_headers
    ) {
        this.transport = transport;
        this.tunnel_headers = tunnel_headers;
        this.expiration_epoch_ms = expiration_epoch_ms;
    }

    /**
     * <pre>
     * Function return number of milliseconds till the subscription expiration if expiration_epoch_ms is set.
     *
     * Function returns a positive number of milliseconds till subscription expiration,
     * or negative number indicating how long ago the subscription expired
     * or null if "expiration_epoch_ms" value is not set.
     *
     * </pre>
     *
     * @return
     */
    public Long time_till_subscription_expiration_ms() {
        // Check if header is set to long value
        if (expiration_epoch_ms != null && expiration_epoch_ms != 0) {
            // Get current epoch ms time
            long now_epoch_ms = System.currentTimeMillis();
            // Reuturn number of ms till expiration (positive => not expired, negative => expired)
            return expiration_epoch_ms - now_epoch_ms;
        } else {
            // The "expiration_epoch_ms" value is not set.
            return null;
        }
    }
}
