package ru.quipy.payments.logic.payment.entities

import java.util.UUID

class PaymentProcessingResult(
    val submittedAt: Long,
    val processedAt: Long,
    val transactionId: UUID?,
    val reason: String?,
    val success: Boolean,
)