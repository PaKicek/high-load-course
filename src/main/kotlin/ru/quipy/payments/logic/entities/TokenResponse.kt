package ru.quipy.payments.logic.entities

data class TokenResponse(
    val accessToken: String,
    val refreshToken: String
)