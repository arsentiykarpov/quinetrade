package com.quinetrade.signals

import com.quinetrade.sources.binance.stream.AggTradeStreamSource
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

class WhalesSignal(val aggTrade: AggTradeStreamSource) {


  fun observe() = flow<Int> {
    val coroutineScope = CoroutineScope(Dispatchers.IO)
    coroutineScope.launch {
      aggTrade.observeStream().collect {

      }
    }
    
  }
  /*
   Sliding window, e.g. 1–5 minutes.

Track:

sum of notional buys

sum of notional sells

count of trades ≥ some per-trade threshold

Trigger “whale event” when:

total buy notional in window ≥ X, or

total sell notional in window ≥ Y, or

N whale trades within M seconds

This catches a whale splitting orders into many chunks.
*/
}
