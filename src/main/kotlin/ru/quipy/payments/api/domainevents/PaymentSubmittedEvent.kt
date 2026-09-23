package ru.quipy.payments.api.domainevents

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID

@DomainEvent(name = "PAYMENT_SUBMITTED_EVENT")
class PaymentSubmittedEvent(
    val paymentId: UUID,
    val success: Boolean,
    val orderId: UUID,
    val transactionId: UUID,
    val startedAt: Long,
    val spentInQueueDuration: Duration,
    createdAt: Long = System.currentTimeMillis(),
) : Event<PaymentAggregate>(
    name = "PAYMENT_SUBMITTED_EVENT",
    createdAt = createdAt,
)