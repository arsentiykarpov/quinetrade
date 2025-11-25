package com.quinetrade

import com.quintrade.signals.TradeSignal
import com.quintrade.sources.binance.stream.AggTradeStreamSource
import com.quintrade.sources.binance.stream.OrderBookStreamSource
import io.ktor.server.application.*
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(args)
}

fun Application.initClient() {
    val log = LoggerFactory.getLogger("App logger")
    var spreadStream = OrderBookStreamSource()
    var tradeStream = AggTradeStreamSource()
    var tradeSignals: TradeSignal = TradeSignal(spreadStream, tradeStream, 5_000, log)
    val handler = CoroutineExceptionHandler { _, e -> log.error("Coroutine error", e) }
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO + handler)

    // Start producers
    spreadStream.poll(scope)
    tradeStream.poll(scope)

    scope.launch {
        try {
            tradeSignals.aggWindows().collect {
            }
        } catch (e: Exception) {
        } finally {
            tradeSignals.stop()
            scope.cancel()
        }
    }
}

fun Application.module() {
    configureSockets()
    configureSerialization()
    configureMonitoring()
    configureSecurity()
    configureHTTP()
    initClient()
    //configureRouting()
}
