package ru.quipy.common.utils.window

import java.util.concurrent.Semaphore

class OngoingWindow(maxWinSize: Int) {
    private val window = Semaphore(maxWinSize)

    fun acquire() {
        window.acquire()
    }

    fun release() = window.release()

    fun awaitingQueueSize() = window.queueLength
}