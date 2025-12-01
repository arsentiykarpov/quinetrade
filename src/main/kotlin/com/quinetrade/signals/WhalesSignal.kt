package com.quinetrade.signals

import com.quinetrade.sources.binance.stream.AggTradeStreamSource
import com.quintrade.sources.binance.data.AggTrade
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch

class WhalesSignal(val aggTrade: AggTradeStreamSource) {

  fun observe() = flow<Double> {
    val coroutineScope = CoroutineScope(Dispatchers.IO)
    coroutineScope.launch {
      aggTrade.observeStream().collect {
        if (it.isWhaleSell()) {
          emit(it.aggTrade!!.q.toDouble()) //TODO: error handling
        }
      }
    }
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

fun AggTrade.isWhaleSell(thresholdUsd: Double = 100_000.0): Boolean {
  val price = aggTrade!!.p.toDouble()
  val qty = aggTrade!!.q.toDouble()
  val quote = price * qty

  return aggTrade!!.m && quote >= thresholdUsd
}
