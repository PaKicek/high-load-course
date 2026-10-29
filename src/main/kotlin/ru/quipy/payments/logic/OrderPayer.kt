package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.ratelimiter.RateLimiter
import ru.quipy.common.utils.ratelimiter.implementation.LeakingBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.payment.service.PaymentService
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Service
class OrderPayer {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

        private const val IN_FLIGHT_FACTOR = 0.6
        private const val MIN_RETRY_AFTER_MS = 200L
        private const val MAX_RETRY_AFTER_MS = 1000L
        private const val PAYMENT_CORE_EXECUTOR_THREADS = 128
        private const val PAYMENT_MAX_EXECUTOR_THREADS = 256
        private const val PAYMENT_EXECUTOR_KEEP_ALIVE_SECONDS = 60L

        private const val INGRESS_RATE_PER_SEC = 11L
        private const val INGRESS_BUCKET_SIZE = 33
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        PAYMENT_CORE_EXECUTOR_THREADS,
        PAYMENT_MAX_EXECUTOR_THREADS,
        PAYMENT_EXECUTOR_KEEP_ALIVE_SECONDS,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val inFlightCount = AtomicInteger(0)

    private val ingressRateLimiter: RateLimiter = LeakingBucketRateLimiter(
        rate = INGRESS_RATE_PER_SEC,
        window = Duration.ofSeconds(1),
        bucketSize = INGRESS_BUCKET_SIZE,
    )

    fun tryReserveSlot(deadline: Long): Boolean {
        if (!ingressRateLimiter.tick()) return false

        val remainingMs = deadline - System.currentTimeMillis()
        if (remainingMs <= 0) return false
        val throughputPerMs = paymentService.totalThroughputPerMs()
        if (throughputPerMs <= 0.0) return false
        val avgProcessingMs = paymentService.averageProcessingTimeMs()
        if (avgProcessingMs <= 0) return false

        val accountCapacity = (throughputPerMs * remainingMs).toLong()
        val executorCapacity = PAYMENT_CORE_EXECUTOR_THREADS * remainingMs / avgProcessingMs

        val capacity = (minOf(accountCapacity, executorCapacity) * IN_FLIGHT_FACTOR).toLong()
        if (capacity <= 0) return false

        while (true) {
            val current = inFlightCount.get()
            if (current >= capacity) return false
            if (inFlightCount.compareAndSet(current, current + 1)) return true
        }
    }

    fun retryAfterMs(deadline: Long): Long {
        val remainingMs = deadline - System.currentTimeMillis()
        if (remainingMs <= 0) return MIN_RETRY_AFTER_MS
        val throughputPerMs = paymentService.totalThroughputPerMs()
        if (throughputPerMs <= 0.0) return MIN_RETRY_AFTER_MS
        val avgProcessingMs = paymentService.averageProcessingTimeMs()
        if (avgProcessingMs <= 0) return MIN_RETRY_AFTER_MS

        val accountCapacity = (throughputPerMs * remainingMs).toLong()
        val executorCapacity = PAYMENT_CORE_EXECUTOR_THREADS * remainingMs / avgProcessingMs

        val capacity = (minOf(accountCapacity, executorCapacity) * IN_FLIGHT_FACTOR).toLong()
        val excess = (inFlightCount.get() - capacity).coerceAtLeast(1L)
        val waitMs = (excess / throughputPerMs).toLong()

        val maxAllowed = (remainingMs - 200L).coerceAtLeast(MIN_RETRY_AFTER_MS)
        return waitMs.coerceIn(MIN_RETRY_AFTER_MS, minOf(MAX_RETRY_AFTER_MS, maxAllowed)) + ThreadLocalRandom.current().nextLong(0L, 100L)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        try {
            paymentExecutor.submit {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(paymentId, orderId, amount)
                    }
                    logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                } finally {
                    inFlightCount.decrementAndGet()
                }
            }
        } catch (e: Exception) {
            inFlightCount.decrementAndGet()
            throw e
        }
        return createdAt
    }
}