package ru.quipy.payments.api.domainevents

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID

@DomainEvent(name = "PAYMENT_PROCESSED_EVENT")
class PaymentProcessedEvent(
    val paymentId: UUID,
    val success: Boolean,
    val orderId: UUID,
    val submittedAt: Long,
    val processedAt: Long,
    val amount: Int,
    val transactionId: UUID?,
    val reason: String?,
    val spentInQueueDuration: Duration,
    createdAt: Long = System.currentTimeMillis(),
) : Event<PaymentAggregate>(
    name = "PAYMENT_PROCESSED_EVENT",
    createdAt = createdAt,
)