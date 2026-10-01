package ru.quipy.common.utils.window

import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class OngoingWindow(maxWinSize: Int) {
    private val window = Semaphore(maxWinSize)

    fun acquire() {
        window.acquire()
    }

    fun tryAcquire(timeout: Duration): Boolean =
        try {
            window.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }

    fun release() = window.release()

    fun awaitingQueueSize() = window.queueLength
}