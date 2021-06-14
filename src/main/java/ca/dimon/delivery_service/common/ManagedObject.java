package ca.dimon.delivery_service.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.HashMap;

/**
 * The "Managed Object" is pretty much anything that has unique uri and own
 * configuration and stats. This is a good replacement for the base java
 * "Object" since pretty much anything we use in our day life should be uniquely
 * named/addressable and it would be awesome to be able to make object carry
 * some stats. For example how many times some of the methods were used, how
 * many errors encountered etc.
 *
 */
public class ManagedObject {

    public String mime_type;
    private String uri;
    public HashMap<String, Object> config = new HashMap<>();
    public HashMap<String, Long> stats = new HashMap<>();

    // The constructor will generate us the default unique (good enough) uri string
    // so all the classes that didn't bother to generate any "human-readable" somewhat
    // more meaningful uri will be good to go w/o additional efforts.
    public ManagedObject() {
        generate_default_uri();
    }

    public HashMap<String, Object> get_config() {
        return config;
    }

    public HashMap<String, Long> get_stats() {
        return stats;
    }

    /**
     * Get stats value by given key (String). If no such stat key exists, return
     * null.
     *
     * @param stats_key
     * @return
     */
    public Long get_stats_value(String stats_key) {
        return stats.get(stats_key);
    }

    /**
     * Simply increment stats value by given stats_key.
     *
     * @param stats_key
     */
    public void increment_stats(String stats_key) {
        increment_stats(stats_key, 1);
    }

    /**
     * Increment stats value by given stats_key, add given value. (note: in case
     * of negative values the stats value will go down of course:)
     *
     * @param stats_key
     * @param value
     */
    public void increment_stats(String stats_key, long value) {
        // Check if such stats_key already present in stats hashmap
        Long stats_value = stats.get(stats_key);
        if (stats_value == null) {
            // Not yet exist, create new
            stats.put(stats_key, value);
        } else {
            // Allready exists, simply increment by given value
            stats.put(stats_key, stats_value + value);
        }
    }

    /**
     * Each transort-able class instance must have uri (unique resource
     * identifier), which will be used for the messages routing
     * purposes.Basically uri is a unique string (does not have to be
     * human-readable, but it would be easier to understand what's goes where if
     * we (humans) can also read it. It must be never parsed back! Just an
     * unique string. All the transportable classes are free to override
     * "get_uri()" function, but here is the "good enough" default
     * implementation, which will never generate 2 identical uri's for 2
     * different instances of any kind!
     *
     * @return unique resource identifier (unique across currently running
     * process.
     */
    public String get_uri() {
        if (uri.length() == 0) {
            generate_default_uri();
        }
        return uri;
    }

    /**
     * Generate unique "per instance" default uri. Example: "Foo@1234abcd"
     */
    public void generate_default_uri() {
        generate_default_uri("");
    }

    /**
     * Generate unique "per instance" default uri.Same as
     * generate_default_uri(), but you can specify prefix for the generated
     * string uri.
     *
     * @param prefix
     */
    public void generate_default_uri(String prefix) {
        this.uri = prefix + Aid.get_instance_identity_hashcode(this);
    }

    /**
     * If one doesn't like the default uri value (like for example
     * "DeliveryServiceDemoClient@723279cf") then one can assign any custom
     * string as instance uri using this method.
     *
     * @param uri
     */
    public void set_uri(String uri) {
        this.uri = uri;
    }

    /**
     * <pre>
     * to_json() is useful when we need to serialize the object before sending it over serial line (parsable + human readable).
     * Use cases examples: web-server response to client include request as json string,
     * in case of error it is useful to show whole object in the logs (including all fields) etc.
     *
     * </pre>
     */
    public String to_json() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
//        String pretty_json = gson.toJson(toJsonObject());
        String pretty_json = gson.toJson(this);
        return pretty_json;
    }

//    /**
//     * </pre> toJsonObject() generates JSON representation of the instance of
//     * this class. Note, this JSON representation is not complete and can not be
//     * used to fully serialize and then correctly de-serialize instance. We have
//     * to use custom adapter because GSON would fail to build JSON
//     * representation of object with cyclic references.
//     *
//     * Note: toJsonObject was used by "MyGSONTypeAdapter_" (see:
//     * speculant/src/main/java/ca/dimon/speculant/request/MyGSONTypeAdapter_Contract.java)
//     * </pre>
//     */
//    public JsonObject toJsonObject() {
//        JsonObject json_object = new JsonObject();
//        return toJsonObject(json_object);
//    }
//
//    @Override
//    public JsonObject toJsonObject(JsonObject json_object) {
//        super.toJsonObject(json_object);
//
////        // If historical bars present, add them into the json_object (forgot why we do this manually)
////        if (historical_data_bars.size() != 0) {
////
////            Gson gson = new GsonBuilder().setPrettyPrinting().create();
////            // http://stackoverflow.com/questions/5813434/trouble-with-gson-serializing-an-arraylist-of-pojos
////            JsonElement jsonElement = gson.toJsonTree(historical_data_bars);  // http://stackoverflow.com/questions/18335214/how-to-add-arrayliststring-to-json-array-keeping-type-safety-in-mind
////            json_object.add("bars", jsonElement);
////
////            return json_object;
////
////        }
//        return json_object;
//    }
    // also TODO:
    // increment_stats_value(key, increment = 1)
    // on_error(error_details) -> stats, log
    // chage_state_to(state) - already implemented in ActiveRequest? TODO: move it here, so it is available to all other children!
}
