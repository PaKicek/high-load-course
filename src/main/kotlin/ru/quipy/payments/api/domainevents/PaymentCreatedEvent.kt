package ru.quipy.payments.api.domainevents

import ru.quipy.core.annotations.DomainEvent
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import java.util.UUID

@DomainEvent(name = "PAYMENT_CREATED_EVENT")
class PaymentCreatedEvent(
    val paymentId: UUID,
    val orderId: UUID,
    val amount: Int,
    createdAt: Long = System.currentTimeMillis(),
) : Event<PaymentAggregate>(
    name = "PAYMENT_CREATED_EVENT",
    createdAt = createdAt,
)