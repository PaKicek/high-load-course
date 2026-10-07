package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.payment.service.PaymentService
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Service
class OrderPayer {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

        private const val IN_FLIGHT_FACTOR = 0.9
        private const val RETRY_AFTER_MS = 1000L
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val inFlightCount = AtomicInteger(0)

    fun canAcceptPayment(deadline: Long): Boolean {
        val remainingMs = deadline - System.currentTimeMillis()
        if (remainingMs <= 0) return false

        val throughputPerMs = paymentService.totalThroughputPerMs()
        if (throughputPerMs <= 0.0) return false

        val capacity = (throughputPerMs * remainingMs * IN_FLIGHT_FACTOR).toLong()
        val current = inFlightCount.get()

        val canAccept = current < capacity
        if (!canAccept) {
            logger.debug(
                "Rejecting payment: inFlight={}, capacity={}, remainingMs={}, throughputPerMs={}",
                current, capacity, remainingMs, throughputPerMs
            )
        }
        return canAccept
    }

    fun retryAfterMs(): Long = System.currentTimeMillis() + RETRY_AFTER_MS

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        inFlightCount.incrementAndGet()
        paymentExecutor.submit {
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } finally {
                inFlightCount.decrementAndGet()
            }
        }
        return createdAt
    }
}