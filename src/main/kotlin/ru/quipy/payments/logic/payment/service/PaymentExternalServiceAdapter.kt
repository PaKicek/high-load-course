package ru.quipy.payments.logic.payment.service

import java.util.UUID

/**
 * Adapter for external payment system. Represents the account in the external system.
 *
 * !!! You can extend the interface with additional methods if needed. !!!
 */
interface PaymentExternalServiceAdapter {
    fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)
    fun name(): String
    fun price(): Int
    fun isEnabled(): Boolean
}