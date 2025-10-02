package com.quinetrade

import io.ktor.server.application.*
import com.quinetrade.sources.binance.stream.*
import kotlinx.coroutines.*

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(args)
 }

fun Application.initClient() {
    val client = NetClient()
    val wsPoll = BnsOrderBookPoll(client.client)
    val coroutineScope = CoroutineScope(Dispatchers.IO)
    coroutineScope.launch {
      wsPoll.pollTrade()
    }
}

fun Application.module() {
    configureSockets()
    configureSerialization()
    configureMonitoring()
    configureSecurity()
    configureHTTP()
    initClient()
    configureRouting()
}
