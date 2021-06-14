package ca.dimon.delivery_service.common;

import ca.dimon.aeronmessaging.client.AeronMessagingClientConfiguration;
import ca.dimon.aeronmessaging.client.ImmutableAeronMessagingClientConfiguration;
import ca.dimon.aeronmessaging.server.AeronMessagingServerConfiguration;
import ca.dimon.aeronmessaging.server.ImmutableAeronMessagingServerConfiguration;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * <pre>
 * Class AeronMessagingConfiguration helps us to support 3 use-cases:
 *   - 1-of-3) define AeronMessagingServerConfiguration
 *   - 2-of-3) AeronMessagingClientConfiguration
 *   - 3-of-3) disable Aeron (use case: there are no need for any aeron client-server components even started
 *             if we don't want/expect to serve any remote clients (no clients from other processes).
 * 
 * </pre>
 */
public class AeronMessagingConfiguration {

    /**
     * Constructor disables - use of of the 2 static factories available: -
     * create_aeron_server_configuration or - create_aeron_client_configuration
     */
    private AeronMessagingConfiguration() {

    }

    public enum ROLE {
        CLIENT,
        SERVER
    }

    private ROLE role;

    public ROLE get_role() {
        return role;
    }

    // We eihter create a server configuration or client configuration.
    public AeronMessagingServerConfiguration aeron_server_configuration;
    public AeronMessagingClientConfiguration aeron_client_configuration;

    /**
     * One of 2 available static factories to crate an AeronSettings instance.
     *
     * @param directory_str
     * @param local_address_str
     * @param local_initial_data_port
     * @param local_initial_control_port
     * @param local_clients_base_port
     * @param client_count
     * @return
     * @throws java.lang.Exception
     */
    public static AeronMessagingConfiguration create_aeron_server_configuration(
            String directory_str,
            String local_address_str,
            int local_initial_data_port,
            int local_initial_control_port,
            int local_clients_base_port,
            int client_count
    ) throws Exception {

        AeronMessagingConfiguration instance = new AeronMessagingConfiguration();

        // Turn String path to the directory into Path type
        Path directory = Paths.get(directory_str);

        // Turn given local address given as a string (usually "127.0.0.1" or "0.0.0.0") into InetAddress type
        InetAddress local_address = InetAddress.getByName(local_address_str);

        instance.aeron_server_configuration
                = ImmutableAeronMessagingServerConfiguration.builder()
                        .baseDirectory(directory)
                        .localAddress(local_address)
                        .localInitialPort(local_initial_data_port)
                        .localInitialControlPort(local_initial_control_port)
                        .localClientsBasePort(local_clients_base_port)
                        .clientMaximumCount(client_count)
                        .maximumConnectionsPerAddress(3)
                        .build();

        // Since we're building "server" configuration, let's give this instance of AeronSettings a server role
        instance.role = ROLE.SERVER;
        return instance;
    }

    /**
     * Second of 2 available static factories to crate an AeronSettings
     * instance.
     *
     * @param directory_str
     * @param remote_address_str
     * @param remote_data_port
     * @param remote_control_port
     * @return
     * @throws java.net.UnknownHostException
     */
    public static AeronMessagingConfiguration create_aeron_client_configuration(
            String directory_str,
            String remote_address_str,
            int remote_data_port, //  Integer.parseUnsignedInt(args[2]);
            int remote_control_port //  Integer.parseUnsignedInt(args[3]);
    ) throws UnknownHostException {
        AeronMessagingConfiguration instance = new AeronMessagingConfiguration();

        // Turn String path to the directory into Path type
        Path directory = Paths.get(directory_str);

        // Turn given local address given as a string (usually "127.0.0.1" or "0.0.0.0") into InetAddress type
        InetAddress remote_address = InetAddress.getByName(remote_address_str);

        instance.aeron_client_configuration
                = ImmutableAeronMessagingClientConfiguration.builder()
                        .baseDirectory(directory)
                        .remoteAddress(remote_address)
                        .remoteInitialControlPort(remote_control_port)
                        .remoteInitialPort(remote_data_port)
                        .build();
        instance.role = ROLE.CLIENT;
        return instance;
    }    
}
