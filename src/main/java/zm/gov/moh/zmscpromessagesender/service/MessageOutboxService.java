package zm.gov.moh.zmscpromessagesender.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageOutboxService {

    private final DatabaseClient databaseClient;

    /**
     * Atomically claims a pending message by setting its status to "processing".
     * Returns true only if the row was actually updated (i.e. status was still "pending").
     */
    public Mono<Boolean> claimMessage(UUID oid) {
        return databaseClient.sql("""
                UPDATE MessageOutboxes
                SET Status = 'processing'
                WHERE Oid = :oid AND Status = 'pending'
                """)
                .bind("oid", oid)
                .fetch()
                .rowsUpdated()
                .map(count -> count > 0);
    }

    /**
     * Marks a message as sent and records the processed timestamp.
     */
    public Mono<Void> markAsSent(UUID oid) {
        return databaseClient.sql("""
                UPDATE MessageOutboxes
                SET Status = 'sent', ProcessedAt = :processedAt
                WHERE Oid = :oid
                """)
                .bind("oid", oid)
                .bind("processedAt", LocalDateTime.now())
                .fetch()
                .rowsUpdated()
                .doOnNext(_ -> log.debug("Marked message {} as sent", oid))
                .then();
    }

    /**
     * Marks a message as failed and increments its retry counter.
     */
    public Mono<Void> markAsFailed(UUID oid) {
        return databaseClient.sql("""
                UPDATE MessageOutboxes
                SET Status = 'failed', RetryCount = RetryCount + 1
                WHERE Oid = :oid
                """)
                .bind("oid", oid)
                .fetch()
                .rowsUpdated()
                .doOnNext(count -> log.warn("Marked message {} as failed", oid))
                .then();
    }
}
