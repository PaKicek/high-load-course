package ru.quipy.common.utils.ratelimiter

interface RateLimiter {
    fun tick(): Boolean
}