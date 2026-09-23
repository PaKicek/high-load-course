package ru.quipy.common.utils.ratelimiter.implementation

import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ratelimiter.RateLimiter
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

class FixedWindowRateLimiter(
    private val rate: Int,
    private val window: Long,
    private val timeUnit: TimeUnit = TimeUnit.MINUTES,
): RateLimiter {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(FixedWindowRateLimiter::class.java)
        private val counter = AtomicInteger(0)

        fun makeRateLimiter(accountName: String, rate: Int, timeUnit: TimeUnit = TimeUnit.SECONDS): io.github.resilience4j.ratelimiter.RateLimiter {
            val config = RateLimiterConfig.custom()
                .limitRefreshPeriod(if (timeUnit == TimeUnit.SECONDS) Duration.ofSeconds(1) else Duration.ofMinutes(1))
                .limitForPeriod(rate)
                .timeoutDuration(Duration.ofMillis(5))
                .build()

            val rateLimiterRegistry = RateLimiterRegistry.of(config)

            return rateLimiterRegistry.rateLimiter("rateLimiter:${accountName}")
        }
    }

    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private var semaphore = Semaphore(rate)
    private val semaphoreNumber = counter.getAndIncrement()
    private var start = System.currentTimeMillis()
    private var nextExpectedWakeUp = start + timeUnit.toMillis(window)

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            start = System.currentTimeMillis()
            nextExpectedWakeUp = start + timeUnit.toMillis(window)

            val permitsToRelease = rate - semaphore.availablePermits()
            repeat(permitsToRelease) {
                runCatching {
                    semaphore.release()
                }.onFailure { th -> logger.error("Failed while releasing permits", th) }
            }
            logger.trace("Semaphore ${semaphoreNumber}. Released $permitsToRelease permits")

            delay((nextExpectedWakeUp - System.currentTimeMillis()).milliseconds)
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    override fun tick() = semaphore.tryAcquire()

    fun tickBlocking() = semaphore.acquire()
}