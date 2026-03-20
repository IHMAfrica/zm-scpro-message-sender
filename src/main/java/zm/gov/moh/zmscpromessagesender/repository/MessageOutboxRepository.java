package zm.gov.moh.zmscpromessagesender.repository;

import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;
import zm.gov.moh.zmscpromessagesender.model.MessageOutbox;

import java.util.UUID;

public interface MessageOutboxRepository extends R2dbcRepository<MessageOutbox, UUID> {

    Flux<MessageOutbox> findByStatus(String status);
}
