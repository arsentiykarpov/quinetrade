package com.quinetrade.sources.binance.stream

import java.util.TreeMap
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.serialization.*
import kotlinx.serialization.json.*
import kotlin.math.abs

class OrderBookL2() {
    val bids = TreeMap<Double, Double>(compareByDescending { it })
    val asks = TreeMap<Double, Double>()

    fun apply(side: Char, price: Double, qty: Double) {
        val book = if (side == 'b') bids else asks
        if (qty == 0.0) book.remove(price) else book[price] = qty
    }

    fun bestBid() = bids.entries.firstOrNull()?.toPair()
    fun bestAsk() = asks.entries.firstOrNull()?.toPair()

    fun mid(): Double? {
        val bb = bestBid()?.first ?: return null
        val aa = bestAsk()?.first ?: return null
        return (bb + aa) / 2.0
    }

    fun obi(levels: Int = 10): Double? {
        if (bids.isEmpty() || asks.isEmpty()) return null
        var vb = 0.0; var va = 0.0
        bids.entries.take(levels).forEach { vb += it.value }
        asks.entries.take(levels).forEach { va += it.value }
        val den = vb + va
        return if (den == 0.0) null else (vb - va) / den
    }

    fun spreadAbs(): Double? {
        val bb = bestBid()?.first ?: return null
        val aa = bestAsk()?.first ?: return null
        return aa - bb
    }
}


