package zm.gov.moh.zmscpromessagesender.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import zm.gov.moh.zmscpromessagesender.grpc.MessageServiceGrpc;
import zm.gov.moh.zmscpromessagesender.grpc.SendMessageRequest;
import zm.gov.moh.zmscpromessagesender.grpc.SendMessageResponse;
import zm.gov.moh.zmscpromessagesender.model.MessageOutbox;

@Slf4j
@Service
public class MessageDispatcherService {

    private final MessageServiceGrpc.MessageServiceBlockingStub disaStub;
    private final MessageServiceGrpc.MessageServiceBlockingStub elmisStub;
    private final MessageOutboxService outboxService;

    public MessageDispatcherService(
            @Qualifier("disaStub") MessageServiceGrpc.MessageServiceBlockingStub disaStub,
            @Qualifier("elmisStub") MessageServiceGrpc.MessageServiceBlockingStub elmisStub,
            MessageOutboxService outboxService) {
        this.disaStub = disaStub;
        this.elmisStub = elmisStub;
        this.outboxService = outboxService;
    }

    /**
     * Dispatches a message to the appropriate gRPC endpoint based on its MessageType,
     * then updates the outbox record to "sent" or "failed" accordingly.
     */
    public Mono<Void> dispatch(MessageOutbox message) {
        SendMessageRequest request = SendMessageRequest.newBuilder()
                .setCorrelationId(message.getCorrelationId().toString())
                .setPayload(message.getPayload() != null ? message.getPayload() : "")
                .setTargetSystem(message.getTargetSystem() != null ? message.getTargetSystem() : "")
                .setMessageType(message.getMessageType() != null ? message.getMessageType() : "")
                .build();

        return Mono.fromCallable(() -> sendViaGrpc(message.getTargetSystem(), request))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(response -> {
                    log.info("Message {} dispatched successfully [type={}, acknowledged={}]",
                            message.getOid(), message.getMessageType(), response.getAcknowledged());
                    return outboxService.markAsSent(message.getOid());
                })
                .onErrorResume(e -> {
                    log.error("Failed to dispatch message {} [type={}]: {}",
                            message.getOid(), message.getMessageType(), e.getMessage());
                    return outboxService.markAsFailed(message.getOid());
                });
    }

    private SendMessageResponse sendViaGrpc(String targetSystem, SendMessageRequest request) {
        if (targetSystem == null) {
            throw new IllegalArgumentException("Target System is null for message " + request.getCorrelationId());
        }
        return switch (targetSystem.toLowerCase()) {
            case "disa" -> disaStub.sendMessage(request);
            case "elmis" -> elmisStub.sendMessage(request);
            default -> throw new IllegalArgumentException("Unknown target system: " + targetSystem);
        };
    }
}
