package ru.quipy.payments.logic.payment.service

import java.util.*

interface PaymentService {
    /**
     * Submit payment request to some external service.
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)
}