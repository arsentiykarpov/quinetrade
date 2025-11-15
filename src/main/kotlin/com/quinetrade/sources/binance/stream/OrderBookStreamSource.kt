package com.quintrade.sources.binance.stream

import com.quinetrade.sources.binance.data.DepthDiff
import com.quinetrade.sources.binance.stream.BaseStreamSource
import com.quinetrade.sources.binance.stream.OrderBookL2
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import org.slf4j.Logger
import java.awt.print.Book
import kotlin.apply

class OrderBookStreamSource() : BaseStreamSource<OrderBookStreamSource.BookStats>(BookStats::class,
"wss://stream.binance.com:9443/ws/btcusdt@depth@100ms") {

    var lastUpdateId: Long = 0 // выставьте после snapshot
    val book = OrderBookL2()
    override fun parseTextFrame(frame: Frame.Text): BookStats {
        val json = Json { ignoreUnknownKeys = true }
        val diff = json.decodeFromString<DepthDiff>(frame.readText())

        if (diff.prevFinalUpdateId != null && diff.prevFinalUpdateId != lastUpdateId) {
            throw RuntimeException("GAP detected, need resync")
        }

        diff.b.forEach { (p, q) -> book.apply('b', p.toDouble(), q.toDouble()) }
        diff.a.forEach { (p, q) -> book.apply('a', p.toDouble(), q.toDouble()) }
        lastUpdateId = diff.finalUpdateId
        val spread = book.spreadAbs()
        val mid = book.mid()
        val obi = book.obi(10)
        return BookStats(diff.eventTime!!, spread!!, obi!!, mid!!, null)
    }


    override fun error(): BookStats {
        return BookStats(0,0.0,0.0,0.0, "error")
    }
    data class BookStats(
        val t: Long,
        val spread: Double,
        val obi: Double,
        val mid: Double,
        var error: String?
    )
}
