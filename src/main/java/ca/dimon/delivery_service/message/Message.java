package ca.dimon.delivery_service.message;

import ca.dimon.delivery_service.message.MessageHeaderMimeType;
import ca.dimon.delivery_service.common.ManagedObject;
import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Message extends ManagedObject {

    // headers: key is a header name (always a String), value - any object (example: String, Long, Interger, etc.)
    public HashMap<String, Object> headers = new HashMap<>();
    public Object body;
    public static Long default_transaction_expiration_ms = 1000L;

    public Message() {
    }

    /**
     * Static factory method to create new Message instance for the "request"
     * message. See for details:
     * https://www.baeldung.com/java-constructors-vs-static-factory-methods
     *
     * @param from
     * @param to
     * @param body
     * @return
     */
    public static Message create_new_request(String from, String to, Object body) {
        return create_new_request(from, to, body, null);
    }

    public static Message create_new_request(String from, String to, Object body, Long transaction_expiration_epoch_ms) {
        Message message = new Message();

        message.headers.put("mime_type", MessageHeaderMimeType.REQUEST);
        message.headers.put("from", from);
        message.headers.put("to", to);

        // Set transaction expiration time to either default value or specified by the caller
        if (transaction_expiration_epoch_ms != null) {
            message.headers.put("transaction_expiration_epoch_ms", transaction_expiration_epoch_ms);
        } else {
            long now_epoch_ms = System.currentTimeMillis();
            message.headers.put("transaction_expiration_epoch_ms", now_epoch_ms + default_transaction_expiration_ms);
        }

        message.body = body;
        message.mime_type = "speculant/message";
        long now_epoch_ms = System.currentTimeMillis();
        message.headers.put("timestamp_epoch_ms", now_epoch_ms);
        return message;
    }

    /**
     * Static factory method to create new Message instance for the "response"
     * message.
     *
     * @param from
     * @param body
     * @param original_request
     * @return
     */
    public static Message create_new_response(String from, Object body, Message original_request) {
        Message message = new Message();

        message.headers.put("mime_type", MessageHeaderMimeType.RESPONSE);
        message.headers.put("from", from);

        // Get some headers (i.e. "to:" and "transaction_id:") from the original request
        message.headers.put("to", original_request.headers.get("from"));
        message.headers.put("transaction_id", original_request.headers.get("transaction_id"));
        // todo: add here "tunnel-specific ones as well", like: tunnel_headers can be:
        //    - subscriber_transport_uri
        //    - subscriber_aeron_session_id
        // todo: may be it worth copy "all headrers" from the original request? so they all persis? not for now..
        //
        // Preserve the original request header: subscriber_transport_uri
        if (original_request.headers.get("subscriber_transport_uri") instanceof String) {
            String subscriber_transport_uri = original_request.headers.get("subscriber_transport_uri").toString();
            message.headers.put("subscriber_transport_uri", subscriber_transport_uri);
        }

        // Preserve the original request header: subscriber_aeron_session_id
        if (original_request.headers.get("subscriber_aeron_session_id") instanceof String) {
            String subscriber_aeron_session_id = original_request.headers.get("subscriber_aeron_session_id").toString();
            message.headers.put("subscriber_aeron_session_id", subscriber_aeron_session_id);
        }

        // Preserve the original request header: "transaction_expiration_epoch_ms"
        if (original_request.headers.get("transaction_expiration_epoch_ms") instanceof Long) {
            Long transaction_expiration_epoch_ms = (Long)original_request.headers.get("transaction_expiration_epoch_ms");
            message.headers.put("transaction_expiration_epoch_ms", transaction_expiration_epoch_ms);
        }
        
        message.body = body;
        message.mime_type = "speculant/message";
        long now_epoch_ms = System.currentTimeMillis();
        message.headers.put("timestamp_epoch_ms", now_epoch_ms);
        return message;
    }

    /**
     * Static factory method to create new Message instance for the "publish"
     * message.
     *
     * @param from
     * @param to
     * @param body
     * @return
     */
    public static Message create_new_publish(String from, String to, Object body) {
        Message message = new Message();

        message.headers.put("mime_type", MessageHeaderMimeType.PUBLISH);
        message.headers.put("from", from);
        message.headers.put("to", to);
        message.body = body;
        message.mime_type = "speculant/message";
        long now_epoch_ms = System.currentTimeMillis();
        message.headers.put("timestamp_epoch_ms", now_epoch_ms);
        return message;
    }

    public MessageHeaderMimeType header_get_mime_type() {
        try {
            return (MessageHeaderMimeType) headers.get("mime_type");
        } catch (Exception ex) {
            this.increment_stats("error");
            return null;
        }
    }

    /**
     * <pre>
     * Function return number of milliseconds till the message expiration.
     * Function tries to get "transaction_expiration_epoch_ms" header from the message headers.
     * Function returns a positive number of milliseconds till expiration if message is not yet expired,
     * or negative number indicating how long ago message expired
     * or null if "transaction_expiration_epoch_ms" header is not set.
     *
     * </pre>
     *
     * @return
     */
    public Long time_till_transaction_expiration_ms() {
        // Check if header is set to long value
        if (headers.get("transaction_expiration_epoch_ms") instanceof Long) {
            // Get the value from the header
            Long transaction_expiration_epoch_ms = (Long) headers.get("transaction_expiration_epoch_ms");
            // Get current epoch ms time
            long now_epoch_ms = System.currentTimeMillis();
            // Reuturn number of ms till expiration (positive => not expired, negative => expired)
            return transaction_expiration_epoch_ms - now_epoch_ms;
        } else {
            // Can't find "transaction_expiration_epoch_ms" header value, return null.
            return null;
        }
    }
}
