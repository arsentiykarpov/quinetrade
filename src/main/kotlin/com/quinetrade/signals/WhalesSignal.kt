package com.quinetrade.signals

import com.quinetrade.sources.binance.stream.AggTradeStreamSource
import com.quintrade.sources.binance.data.AggTrade
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import org.apache.avro.Schema

class WhalesSignal(val aggTrade: AggTradeStreamSource, val coroutineScope: CoroutineScope) {

    fun observe(): Flow<AggTrade> =
        aggTrade.observeStream().filter {
            it.isWhaleSell()
        }.map {
            it.aggTrade!!
        }


    fun <T> Flow<T>.slidingWindowFast(windowSize: Int): Flow<List<T>> =
        channelFlow {
            val buffer = ArrayDeque<T>(windowSize)
            collect { value ->
                if (buffer.size == windowSize)
                    buffer.removeFirst()
                buffer.addLast(value)
                send(buffer.toList()) // копия наружу
            }
        }

}

val WHALE_SCHEMA: Schema = Schema.Parser().parse(
    """
        {
          "type": "record",
          "name": "WhaleSchema",
          "namespace": "com.quinetrade.model",
          "fields": [
            { "name": "t", "type": "long" },
            { "name": "q", "type": "double" }
          ]
        }
""".trimIndent()
)

fun AggTrade.isWhaleSell(thresholdUsd: Double = 100_000.0): Boolean {
    val price = aggTrade!!.p.toDouble()
    val qty = aggTrade!!.q.toDouble()
    val quote = price * qty

    return aggTrade!!.m && quote >= thresholdUsd
}
