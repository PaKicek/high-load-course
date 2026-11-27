package ru.quipy.payments.logic.payment.service

import java.util.UUID
import java.util.concurrent.CompletableFuture

/**
 * Adapter for external payment system. Represents the account in the external system.
 *
 * !!! You can extend the interface with additional methods if needed. !!!
 */
interface PaymentExternalServiceAdapter {
    fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, attempt: Int): Boolean
    fun performPaymentAsyncFuture(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, attempt: Int): CompletableFuture<Boolean>
    fun name(): String
    fun price(): Int
    fun isEnabled(): Boolean
    fun throughputPerMs(): Double
    fun averageProcessingTimeMs(): Long
}