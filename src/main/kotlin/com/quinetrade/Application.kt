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
    var spreadStream = OrderBookStreamSource(log)
    var tradeStream = AggTradeStreamSource()
    var tradeSignals: TradeSignal = TradeSignal(spreadStream, tradeStream, 5_000, log)
    val handler = CoroutineExceptionHandler { _, e -> log.error("Coroutine error", e) }
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO + handler)

    // Start producers
    scope.launch { spreadStream.poll() }
    scope.launch { tradeStream.poll() }

    scope.launch {
        tradeSignals.aggWindows().collect {
        }
    }
    // Collectors
    scope.launch {
        spreadStream.observeStream().collect {
        }
    }

    scope.launch {
        tradeStream.observeStream().collect {
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
