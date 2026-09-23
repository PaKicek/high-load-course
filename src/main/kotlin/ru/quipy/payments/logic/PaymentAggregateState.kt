package ru.quipy.payments.logic

import ru.quipy.core.annotations.StateTransitionFunc
import ru.quipy.domain.AggregateState
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.domainevents.PaymentCreatedEvent
import ru.quipy.payments.api.domainevents.PaymentProcessedEvent
import ru.quipy.payments.api.domainevents.PaymentSubmittedEvent
import ru.quipy.payments.logic.payment.entities.PaymentProcessingResult
import ru.quipy.payments.logic.payment.entities.PaymentSubmission
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class PaymentAggregateState : AggregateState<UUID, PaymentAggregate> {
    private lateinit var paymentId: UUID
    lateinit var orderId: UUID

    var createdAt: Long = System.currentTimeMillis()
    var updatedAt: Long = System.currentTimeMillis()
    var amount: Int? = null
    var submissions = ConcurrentHashMap<UUID, PaymentSubmission>()
    var processings = ConcurrentHashMap<UUID, PaymentProcessingResult>()

    override fun getId() = paymentId

    @StateTransitionFunc
    fun paymentCreatedApply(event: PaymentCreatedEvent) {
        paymentId = event.paymentId
        orderId = event.orderId
        amount = event.amount
        updatedAt = createdAt
    }

    @StateTransitionFunc
    fun paymentSubmittedApply(event: PaymentSubmittedEvent) {
        submissions[event.transactionId] =
            PaymentSubmission(event.startedAt, event.transactionId, event.success, event.spentInQueueDuration)
        updatedAt = createdAt
    }

    @StateTransitionFunc
    fun paymentSubmittedApply(event: PaymentProcessedEvent) {
        processings[event.transactionId ?: UUID.randomUUID()] = PaymentProcessingResult(
            event.submittedAt,
            event.processedAt,
            event.transactionId,
            event.reason,
            event.success
        )
        updatedAt = createdAt
    }

    fun create(id: UUID, orderId: UUID, amount: Int): PaymentCreatedEvent {
        return PaymentCreatedEvent(
            paymentId = id,
            orderId = orderId,
            amount = amount,
        )
    }

    fun logSubmission(success: Boolean, transactionId: UUID, startedAt: Long, spentInQueueDuration: Duration): PaymentSubmittedEvent {
        return PaymentSubmittedEvent(
            this.getId(), success, this.orderId, transactionId, startedAt, spentInQueueDuration
        )
    }

    fun logProcessing(
        success: Boolean,
        processedAt: Long,
        transactionId: UUID? = null,
        reason: String? = null
    ): PaymentProcessedEvent {
        val submittedAt = this.submissions[transactionId ?: UUID.randomUUID()]?.timeStarted ?: 0
        val spentInQueueDuration = this.submissions[transactionId ?: UUID.randomUUID()]?.spentInQueue ?: Duration.ofMillis(0)

        return PaymentProcessedEvent(
            this.getId(), success, this.orderId, submittedAt, processedAt, this.amount!!, transactionId, reason, spentInQueueDuration
        )
    }
}