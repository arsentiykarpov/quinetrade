package com.quinetrade

import com.quintrade.sources.binance.stream.OrderBookStreamSource
import io.ktor.server.application.*
import kotlinx.coroutines.*
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(args)
 }

fun Application.initClient() {
    val client = NetClient()
    var logger = LoggerFactory.getLogger("test")
    val book = OrderBookStreamSource(logger)
    var coroutineScope = CoroutineScope(Dispatchers.IO)
    coroutineScope.launch {
      book.poll()
      book.observeStream().collect {
        logger.debug(it.toString())
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
