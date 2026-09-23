package ru.quipy.payments.logic.entities

data class CreateUserRequest(
    val name: String,
    val password: String
)