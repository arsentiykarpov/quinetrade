package com.quinetrade

import com.quintrade.signals.TradeSignal
import com.quintrade.sources.binance.stream.AggTradeStreamSource
import com.quintrade.sources.binance.stream.OrderBookStreamSource
import io.ktor.server.application.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

fun Application.configureRouting() = routing() {
    webSocket("/ws") {
        val log = LoggerFactory.getLogger("BinanceWS")
        var spreadStream = OrderBookStreamSource(log)
        var tradeStream = AggTradeStreamSource()
        var tradeSignals: TradeSignal = TradeSignal(spreadStream, tradeStream, 10_000, log)
        tradeSignals.aggWindows().collect { w ->
            val p = WsPoint(
                t = w.windowEnd,
                mid = w.mid,
                spread = w.spread,
                vwap = w.vwap,
                buyShare = w.buyShare,
                logRatio = w.logRatio,
                tb = w.tb,
                ts = w.ts,
                obi = w.obi
            )
            log.debug(p.toString())
            sendSerialized(p)
        }
    }
}

@Serializable
data class WsPoint(
    val t: Long,                 // = windowEnd
    val mid: Double? = null,
    val spread: Double? = null,
    val vwap: Double? = null,
    val buyShare: Double? = null,
    val logRatio: Double? = null,
    val tb: Double? = null,
    val ts: Double? = null,
    val obi: Double? = null
)
