package com.quintrade.sources.binance.stream

import com.quintrade.sources.binance.data.AggTrade
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.coroutineScope
import org.slf4j.Logger

class AggTradeReduce(val stream: AggTradeStreamSource, val log: Logger) {

  suspend fun aggregate(windowMs: Long) = coroutineScope {
      var curBucket = -1L
      var tb = 0.0; var ts = 0.0
      stream.observeStream()
      .filterIsInstance<AggTrade.AggTradeDto>()
      .collect { t -> 
          val b = t.T / windowMs
          var ratio = 0.0
          var start = 0L
          var end = 0L
          if (curBucket == -1L) curBucket = b
          if (b != curBucket) {
              start = curBucket * windowMs
              end   = (curBucket + 1) * windowMs
              ratio = tb / (tb + ts)
              curBucket = b; tb = 0.0; ts = 0.0
          }
          val qty = t.q.toDoubleOrNull() ?: return@collect
          if (t.m) ts += qty else tb += qty
          log.info("[$start..$end] TB=$tb TS=$ts NET=${tb-ts} RATIO=${"%.3f".format(ratio)}")
      }
  }
   

}
