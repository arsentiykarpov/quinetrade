package com.quintrade.signals

import com.quintrade.sources.binance.data.AggTrade
import com.quintrade.sources.binance.stream.AggTradeStreamSource
import com.quintrade.sources.binance.stream.OrderBookStreamSource
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.system.measureNanoTime

class TradeSignal(val orderBook: OrderBookStreamSource, val aggTrade: AggTradeStreamSource, val windowMs: Long) {

    private val buf = ArrayDeque<Double>() // buyShare history
    private var cvd = 0.0
    private val zLen = 60

    suspend fun process() = coroutineScope {
      aggWindows().collect {
        w ->
          val mid = w.mid ?: return@collect
          val vwap = w.vwap ?: mid
          cvd += (w.tb - w.ts)
          val buyShare = w.buyShare
          buf += buyShare
          if (buf.size > zLen) buf.removeFirst()
          val mean = buf.average()
          val variance = buf.map {
            val d = it - mean
            d * d
          }.average()
          val std = sqrt(variance)
          val zBuy = if (buf.size >= 10) (buyShare - mean)/std else 0.0
          val div = vwap - mid
      }
    }

    fun aggWindows(): Flow<WindowAgg> = channelFlow {
        var curBucket = Long.MIN_VALUE
        var tb = 0.0;
        var ts = 0.0
        var vwapNum = 0.0;
        var vwapDen = 0.0
        var spread: Double? = null
        var obi: Double? = null
        var mid: Double? = null

        fun flushIfReady(nextBucket: Long) {
            if (curBucket == Long.MIN_VALUE) {
                curBucket = nextBucket; return
            }
            if (nextBucket != curBucket) {
                val start = curBucket * windowMs
                val end = (curBucket + 1) * windowMs
                val buyShare = tb / (tb + ts)
                val logRatio = ln(tb / ts)
                val vwap = if (vwapDen > 0.0) vwapNum / vwapDen else null
                trySend(
                    WindowAgg(start, end, tb, ts, buyShare, logRatio, vwap, spread, obi, mid)
                )
                curBucket = nextBucket
                tb = 0.0; ts = 0.0
                vwapNum = 0.0; vwapDen = 0.0
                spread = null; obi = null; mid = null
            }
        }

        launch {
            orderBook.observeStream().collect { book ->
                val b = book.t / windowMs
                flushIfReady(b)
                spread = book.spread
                obi = book.obi
                mid = book.mid
            }
        }

        launch {
            aggTrade.observeStream()
                .map {
                    it.aggTrade
                }
                .filterNotNull()
                .collect { trade ->
                    val b = trade.T / windowMs
                    flushIfReady(b)
                    val qty = trade.q.toDoubleOrNull() ?: 0.0
                    val px = trade.p.toDoubleOrNull() ?: 0.0
                    if (trade.m) ts += qty else tb += qty
                    if (px != null) {
                        vwapNum += px * qty
                        vwapDen += qty
                    }

                }
        }

        // Optional: time-based flush to not “hang” if no new events cross a boundary
        launch {
            val tick = ticker(delayMillis = windowMs.toLong(), initialDelayMillis = windowMs.toLong())
            for (unit in tick) {
                if (curBucket != Long.MIN_VALUE) {
                    // pretend we advanced one bucket; this will flush current partial window
                    flushIfReady(curBucket + 1)
                }
            }
        }
    }


    data class WindowAgg(
        val windowStart: Long,
        val windowEnd: Long,
        val tb: Double,
        val ts: Double,
        val buyShare: Double,
        val logRatio: Double,
        val vwap: Double?,
        val spread: Double?,
        val obi: Double?,
        val mid: Double?
    )

    
    data class Enriched(
        val w: WindowAgg,
        val cvd: Double,
        val zBuyShare: Double,
        val vwapMinusMid: Double
    )
}
