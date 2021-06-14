package ca.dimon.delivery_service.common;

import java.util.Collections;

public class Aid {

    /**
     * <pre>
     * Get instance unique "signature". Example: "Foo@4aa298b7" - it consist of simple class name
     * concatenated with "@" followed by 8-character long string represending hex value of the "identity hashcode" of the instance.
     * We can use this method to reliably get predictable uniform class signature even if class has overriden "toString()" method.
     *
     * For details see: https://stackoverflow.com/questions/18396927/how-to-print-the-address-of-an-object-if-you-have-redefined-tostring-method
     * in particular https://stackoverflow.com/a/18397076/7022062
     *
     * </pre>
     *
     * @param instance_of_any_kind
     * @return
     */
    public static String get_instance_identity_hashcode(Object instance_of_any_kind) {
        return instance_of_any_kind.getClass().getSimpleName() + String.format("@%08X", System.identityHashCode(instance_of_any_kind)).toLowerCase();
    }

    // Helper function to sleep given number of milliseconds
    public static void sleep_ms(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ex) {
        }
    }

    // Helper function to sleep given number of nanoseconds :) Or course it is useless and switchign between threads would take longer, but
    // this function magically helped to yield to other threads waiting for the lock to be released (on some synchronized object shared across multiple
    // threads). So even calling sleep_ns(1) between trying to get lock again would make it fair across all participating threads and they
    // will have equal "time share" to grab the lock and used it.
    //
    // Thesea are the methods I played with:
    // Method 1:
    // sleep_ms(1); // this will yield to other thread waiting for the lock, but 1ms
    // might be too long of a sleep.
    //
    // Method 2:
    // Thread.yield(); // does not work as I'd expect.. only yields sometimes, but
    // mostly 1 thread keeping the lock :(
    //
    // Method 3: THE BEST approach is to sleep 100ns (+ expenses on the call itself),
    // that will reliably yield to another thread and not too expensive.
    //
    //
    // https://stackoverflow.com/questions/11498585/how-to-suspend-a-java-thread-for-a-small-period-of-time-like-100-nanoseconds
    public static void sleep_ns(int ns) {
        try {
            Thread.sleep(0, ns);
        } catch (InterruptedException ex) {
        }
    }
    
    public static String pad_string_with_spaces(String s, int total_desired_length){
        String result = s;
        if (s.length() < total_desired_length) {
            result = result + String.join("", Collections.nCopies(total_desired_length - s.length(), " "));
        }
        return result;
    }
}
