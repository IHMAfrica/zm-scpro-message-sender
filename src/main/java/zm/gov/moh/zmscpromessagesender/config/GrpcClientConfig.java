package zm.gov.moh.zmscpromessagesender.config;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zm.gov.moh.zmscpromessagesender.grpc.MessageServiceGrpc;

@Configuration
public class GrpcClientConfig {

    @Value("${grpc.clients.disa.host:localhost}")
    private String disaHost;

    @Value("${grpc.clients.disa.port:9090}")
    private int disaPort;

    @Value("${grpc.clients.elmis.host:localhost}")
    private String elmisHost;

    @Value("${grpc.clients.elmis.port:9095}")
    private int elmisPort;

    @Bean(name = "disaStub", destroyMethod = "")
    public MessageServiceGrpc.MessageServiceBlockingStub disaStub() {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(disaHost, disaPort)
                .usePlaintext()
                .build();
        return MessageServiceGrpc.newBlockingStub(channel);
    }

    @Bean(name = "elmisStub", destroyMethod = "")
    public MessageServiceGrpc.MessageServiceBlockingStub elmisStub() {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(elmisHost, elmisPort)
                .usePlaintext()
                .build();
        return MessageServiceGrpc.newBlockingStub(channel);
    }
}
