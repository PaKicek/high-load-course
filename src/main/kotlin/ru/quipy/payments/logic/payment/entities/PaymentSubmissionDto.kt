package ru.quipy.payments.logic.payment.entities

import java.util.UUID

class PaymentSubmissionDto(
    val timestamp: Long,
    val transactionId: UUID
)