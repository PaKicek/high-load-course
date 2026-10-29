package ru.quipy.common.utils.ratelimiter.implementation

import ru.quipy.common.utils.ratelimiter.RateLimiter
import java.time.Duration

class CompositeRateLimiter(
    private val rl1: RateLimiter,
    private val rl2: RateLimiter,
    private val mode: Mode = Mode.AND,
) : RateLimiter {
    enum class Mode { AND, OR, PRIORITY }

    override fun tick(): Boolean {
        return when (mode) {
            Mode.AND -> {
                if (!rl1.tick()) return false
                if (!rl2.tick()) return false
                true
            }
            Mode.OR -> {
                val p = rl1.tick()
                val s = rl2.tick()
                p || s
            }
            Mode.PRIORITY -> {
                if (rl1.tick()) {
                    true
                } else {
                    rl2.tick()
                }
            }
        }
    }

    override fun tickBlocking() {
        when (mode) {
            Mode.PRIORITY -> {
                while (!rl1.tick()) {
                    if (rl2.tickBlocking().let { true }) return
                }
            }
            else -> {
                while (!tick()) Thread.sleep(1)
            }
        }
    }

    override fun tickBlocking(timeout: Duration): Boolean {
        val deadlineNs = System.nanoTime() + timeout.toNanos()
        return when (mode) {
            Mode.PRIORITY -> {
                if (rl1.tick()) return true
                val remaining = Duration.ofNanos((deadlineNs - System.nanoTime()).coerceAtLeast(0))
                rl2.tickBlocking(remaining)
            }
            else -> {
                while (System.nanoTime() < deadlineNs) {
                    if (tick()) return true
                    Thread.sleep(1)
                }
                tick()
            }
        }
    }

    override fun currentCount(): Long = rl1.currentCount() + rl2.currentCount()
}