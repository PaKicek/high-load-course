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
import java.util.concurrent.LinkedBlockingQueue
import kotlin.time.Duration.Companion.milliseconds

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Int>(bucketSize)
    private val releaseIntervalMs = window.toMillis() / rate

    override fun currentCount(): Long = queue.size.toLong()

    override fun tick(): Boolean {
        return queue.offer(1)
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            delay(releaseIntervalMs.milliseconds)
            queue.poll()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}