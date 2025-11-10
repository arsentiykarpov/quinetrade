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
    val log = LoggerFactory.getLogger("App logger")
    var spreadStream = OrderBookStreamSource(log)
    var tradeStream = AggTradeStreamSource()
    var tradeSignals: TradeSignal = TradeSignal(spreadStream, tradeStream, 10_000, log)
    log.info("TEST")
    var scope = CoroutineScope(Dispatchers.IO)
    scope.launch {
    tradeSignals.aggWindows().collect {
      log.info(it.toString())
    }
    }
    scope.launch {
      spreadStream.poll()
    }
    scope.launch {
      tradeStream.poll()
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
