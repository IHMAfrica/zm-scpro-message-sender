package zm.gov.moh.zmscpromessagesender.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import zm.gov.moh.zmscpromessagesender.model.MessageOutbox;
import zm.gov.moh.zmscpromessagesender.repository.MessageOutboxRepository;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessagePollerService {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);

    private final MessageOutboxRepository repository;
    private final MessageOutboxService outboxService;
    private final MessageDispatcherService dispatcher;

    @PostConstruct
    public void startPolling() {
        log.info("Starting outbox message poller (interval={}ms)", POLL_INTERVAL.toMillis());
        pollContinuously()
                .subscribe(
                        null,
                        error -> log.error("Outbox polling loop terminated unexpectedly", error)
                );
    }

    /**
     * Infinite reactive loop: poll → process all pending → wait → repeat.
     * Uses defer+repeat so each iteration starts fresh, with no tick backpressure.
     */
    private Flux<Void> pollContinuously() {
        return Flux.defer(() ->
                pollOnce()
                        .onErrorResume(e -> {
                            log.error("Error during poll cycle", e);
                            return Mono.empty();
                        })
                        .then(Mono.delay(POLL_INTERVAL))
                        .then()
        ).repeat();
    }

    /**
     * Finds all pending messages, atomically claims each one, then dispatches.
     * The claim step (status: pending → processing) acts as a distributed lock
     * so multiple instances don't double-process the same row.
     */
    private Mono<Void> pollOnce() {
        return repository.findByStatus("pending")
                .flatMap(this::claimAndDispatch)
                .then();
    }

    private Mono<Void> claimAndDispatch(MessageOutbox message) {
        return outboxService.claimMessage(message.getOid())
                .flatMap(claimed -> {
                    if (!claimed) {
                        // Another instance already claimed this message
                        log.debug("Message {} already claimed by another instance, skipping", message.getOid());
                        return Mono.empty();
                    }
                    log.debug("Claimed message {} [type={}], dispatching...", message.getOid(), message.getMessageType());
                    return dispatcher.dispatch(message);
                });
    }
}
