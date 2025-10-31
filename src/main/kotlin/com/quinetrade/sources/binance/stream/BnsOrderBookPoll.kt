package com.quinetrade.sources.binance.stream

import com.quinetrade.sources.binance.data.*
import com.quintrade.sources.binance.stream.AggTradeStreamSource
import com.quintrade.sources.binance.stream.OrderBookStreamSource
import com.quintrade.sources.binance.stream.AggTradeReduce
import com.quintrade.signals.TradeSignal
import io.ktor.client.*
import io.ktor.client.plugins.websocket.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.*
import io.ktor.websocket.*
import org.slf4j.LoggerFactory


class BnsOrderBookPoll(val client: HttpClient) {

    private val streamOberv = MutableSharedFlow<Void>()
    private val log = LoggerFactory.getLogger("BinanceWS")
    private lateinit var tradeSignals: TradeSignal

    suspend fun poll() = coroutineScope {
      var spreadStream = OrderBookStreamSource(log)
      var tradeStream = AggTradeStreamSource()
//      tradeSignals = TradeSignal(spreadStream, tradeStream, 10_000)
    }


}
