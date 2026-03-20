package zm.gov.moh.zmscpromessagesender.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@Table("MessageOutboxes")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageOutbox {

    @Id
    @Column("Oid")
    private UUID oid;

    @Column("TargetSystem")
    private String targetSystem;

    @Column("MessageType")
    private String messageType;

    @Column("Payload")
    private String payload;

    @Column("Status")
    private String status;

    @Column("RetryCount")
    private int retryCount;

    @Column("CreatedAt")
    private LocalDateTime createdAt;

    @Column("ScheduledFor")
    private LocalDateTime scheduledFor;

    @Column("ProcessedAt")
    private LocalDateTime processedAt;

    @Column("CorrelationId")
    private UUID correlationId;
}
