package com.quintrade.trade

import com.quintrade.signals.TradeSignal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.Logger

class TradeStrategy(var tradeSignal: TradeSignal, var coroutineScope: CoroutineScope, var log: Logger) {

  fun apply() {
    coroutineScope.launch {
     tradeSignal.priceLogReturns().collect { //TODO: change aggWindows() to hot flow
       log.info(it.toString())
     }
    }
  }

}
