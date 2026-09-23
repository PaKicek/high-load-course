package ru.quipy.payments.logic.entities

import java.util.UUID

data class User(
    val id: UUID,
    val name: String
)