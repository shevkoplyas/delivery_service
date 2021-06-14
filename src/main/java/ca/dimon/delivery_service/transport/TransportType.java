package ca.dimon.delivery_service.transport;

/**
 * <pre>
 * Usage example:
 *   TransportType transport_type = TransportType.AERON;
 * 
 * </pre>
 */
public enum TransportType {
    LOOPBACK(0),
    LOCAL(1),
    REMOTE_TUNNEL_AERON(2);
    public int numeric_value;

    TransportType(int numVal) {
        this.numeric_value = numVal;
    }

    public int get_numeric_value() {
        return numeric_value;
    }
}
