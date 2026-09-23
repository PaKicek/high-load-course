package ru.quipy.payments.logic.payment.service

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

@Service
class PaymentServiceImpl(
    private val paymentAccounts: List<PaymentExternalServiceAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentServiceImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }
}