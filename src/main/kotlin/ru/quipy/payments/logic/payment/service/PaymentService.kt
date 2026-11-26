package ru.quipy.payments.logic.payment.service

import java.util.*
import java.util.concurrent.CompletableFuture

interface PaymentService {
    /**
     * Submit payment request to some external service.
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, attempt: Int = 0): Boolean
    fun submitPaymentRequestFuture(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, attempt: Int = 0): CompletableFuture<Boolean>
    fun totalThroughputPerMs(): Double
    fun averageProcessingTimeMs(): Long
}