package ru.quipy.common.utils.ratelimiter

import java.time.Duration

interface RateLimiter {
    fun tick(): Boolean

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(1)
        }
    }

    fun tickBlocking(timeout: Duration): Boolean {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            if (tick()) return true
            Thread.sleep(1)
        }
        return tick()
    }
}