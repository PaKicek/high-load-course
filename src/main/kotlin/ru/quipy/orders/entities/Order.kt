package ru.quipy.orders.entities

import java.util.UUID

data class Order(
    val id: UUID,
    val userId: UUID,
    val timeCreated: Long,
    val status: OrderStatus,
    val price: Int,
)