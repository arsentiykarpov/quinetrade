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
    val client = NetClient()
    val log = LoggerFactory.getLogger("BinanceWS")
    var spreadStream = OrderBookStreamSource(log)
    var tradeStream = AggTradeStreamSource()
    var tradeSignals: TradeSignal = TradeSignal(spreadStream, tradeStream, 10_000, log)
    var scope = CoroutineScope(Dispatchers.IO)
    scope.launch {
    spreadStream.poll()
    tradeStream.poll()
    tradeStream.observeStream().collect {
        log.debug(it.toString())
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
