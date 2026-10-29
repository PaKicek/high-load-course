package ru.quipy.common.utils.ratelimiter.implementation

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ratelimiter.RateLimiter
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    private val bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val releaseIntervalMs = window.toMillis() / rate
    private val semaphore = Semaphore(bucketSize)

    init {
        repeat(bucketSize) {
            semaphore.acquireUninterruptibly()
        }
    }

    override fun currentCount(): Long = (bucketSize - semaphore.availablePermits()).toLong()

    override fun tick(): Boolean = semaphore.tryAcquire()

    override fun tickBlocking() = semaphore.acquireUninterruptibly()

    override fun tickBlocking(timeout: Duration): Boolean =
        try {
            semaphore.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            delay(releaseIntervalMs.milliseconds)
            semaphore.release()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}