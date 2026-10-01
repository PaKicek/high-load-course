package ru.quipy.payments.logic.payment.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ratelimiter.implementation.FixedWindowRateLimiter
import ru.quipy.common.utils.window.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.logic.entities.ExternalSysResponse
import ru.quipy.payments.logic.payment.entities.PaymentAccountProperties
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

// Advice: always treat time as a Duration
class PaymentExternalServiceAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalServiceAdapter {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        public fun now() = System.currentTimeMillis()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = FixedWindowRateLimiter(
        rate = rateLimitPerSec,
        window = 1,
        timeUnit = TimeUnit.SECONDS,
    )

    private val parallelWindow = OngoingWindow(parallelRequests)

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var lastReason: String? = null
        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        while (true) {
            val nowMs = now()
            val remaining = deadline - nowMs

            if (remaining <= 0) {
                val reason = lastReason ?: "Deadline exceeded"
                logger.warn("[$accountName] Deadline exceeded for payment $paymentId, reason=$reason")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = reason)
                }
                return
            }

            val waitBudget = Duration.ofMillis(remaining)
            if (!rateLimiter.tickBlocking(waitBudget)) {
                logger.warn("[$accountName] Rate limit wait timeout for payment $paymentId, will retry")
                continue
            }
            if (!parallelWindow.tryAcquire(waitBudget)) {
                logger.warn("[$accountName] Parallel limit wait timeout for payment $paymentId, will retry")
                continue
            }

            try {
                client.newCall(request).execute().use { response ->
                    val rawBody = response.body?.string()
                    val body = try {
                        mapper.readValue(rawBody, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Parse failure for txId: $transactionId, payment: $paymentId, code: ${response.code}, body: $rawBody", e)
                        lastReason = e.message ?: "Parse error"
                        return@use
                    }

                    if (body.result) {
                        logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: true, message: ${body.message}")
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = body.message)
                        }
                        return
                    } else {
                        lastReason = body.message
                        logger.warn("[$accountName] Payment returned false for txId: $transactionId, payment: $paymentId, message: ${body.message}. Will retry if time allows.")
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        lastReason = "Request timeout"
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    }
                    else -> {
                        lastReason = e.message ?: "Request error"
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    }
                }
            } finally {
                parallelWindow.release()
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}