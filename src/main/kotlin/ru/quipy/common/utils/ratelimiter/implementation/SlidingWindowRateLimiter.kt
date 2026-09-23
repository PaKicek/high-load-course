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
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.milliseconds

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }

    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val sum = AtomicLong(0)
    private val queue = PriorityBlockingQueue<Measure>(10_000)

    override fun tick(): Boolean {
        while (true) {
            val curSum = sum.get()
            if (curSum >= rate) return false
            if (sum.compareAndSet(curSum, curSum + 1)) {
                queue.add(Measure(1, System.currentTimeMillis()))
                return true
            }
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    data class Measure(
        val value: Long,
        val timestamp: Long
    ) : Comparable<Measure> {
        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val head = queue.peek()
            val winStart = System.currentTimeMillis() - window.toMillis()
            if (head == null) {
                delay(1L.milliseconds)
                continue
            }
            if (head.timestamp > winStart) {
                delay((head.timestamp - winStart).milliseconds)
                continue
            }
            sum.addAndGet(-1)
            queue.take()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
}