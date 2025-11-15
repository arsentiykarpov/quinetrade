package com.quinetrade.sources.binance.data

import kotlinx.serialization.*
import kotlinx.serialization.json.*

@Serializable
data class DepthDiff(
    @SerialName("U") val firstUpdateId: Long? = null,
    @SerialName("u") val finalUpdateId: Long,
    @SerialName("pu") val prevFinalUpdateId: Long? = null,
    @SerialName("E") val eventTime: Long? = null,
    val b: List<List<String>> = emptyList(), // [price, qty]
    val a: List<List<String>> = emptyList()
)

