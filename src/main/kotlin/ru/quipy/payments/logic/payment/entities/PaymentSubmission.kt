package ru.quipy.payments.logic.payment.entities

import java.time.Duration
import java.util.UUID

class PaymentSubmission(
    val timeStarted: Long,
    val transactionId: UUID,
    val success: Boolean,
    val spentInQueue: Duration,
)