package com.quintrade.signals

import com.quintrade.sources.binance.data.AggTrade
import com.quintrade.sources.binance.stream.AggTradeStreamSource
import com.quintrade.sources.binance.stream.OrderBookStreamSource
import com.quintrade.sources.binance.stream.LocalOutputFile
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.plus
import kotlinx.coroutines.CoroutineScope
import kotlinx.serialization.json.Json
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.OutputFile
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.system.measureNanoTime
import org.slf4j.Logger
import java.nio.file.Paths
import java.lang.System

class TradeSignal(
    val orderBook: OrderBookStreamSource,
    val aggTrade: AggTradeStreamSource,
    val windowMs: Long,
    val log: Logger
) {

    val AGG_WINDOW_SCHEMA: Schema = Schema.Parser().parse(
        """
            {
              "type": "record",
              "name": "WindowAgg",
              "namespace": "com.quinetrade.model",
              "fields": [
                { "name": "windowStart", "type": "long" },
                { "name": "windowEnd", "type": "long" },
                { "name": "tb", "type": "double" },
                { "name": "ts", "type": "double" },
                { "name": "buyShare", "type": "double" },
                { "name": "logRatio", "type": "double" },
                { "name": "vwap", "type": ["null", "double"], "default": null },
                { "name": "spread", "type": ["null", "double"], "default": null },
                { "name": "obi", "type": ["null", "double"], "default": null },
                { "name": "mid", "type": ["null", "double"], "default": null }
              ]
            }
    """.trimIndent()
    )

    val output: OutputFile = LocalOutputFile(Paths.get("/tmp/tradesignal_${System.currentTimeMillis()}.parquet"))
    val json = Json { ignoreUnknownKeys = true }
    var writer = AvroParquetWriter.builder<GenericRecord>(output)
        .withSchema(AGG_WINDOW_SCHEMA)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build() //to lateinit
    val ROTATE_PERIOD_MS = 10_000

    private val buf = ArrayDeque<Double>() // buyShare history
    private var cvd = 0.0
    private val zLen = 60
    private var scope: CoroutineScope? = null

    fun aggWindows(): Flow<WindowAgg> = channelFlow {
        var curBucket = Long.MIN_VALUE
        var tb = 0.0
        var ts = 0.0
        var vwapNum = 0.0
        var vwapDen = 0.0
        var spread: Double? = null
        var obi: Double? = null
        var mid: Double? = null
        var currRotateMs = System.currentTimeMillis()

        fun safeDiv(n: Double, d: Double): Double? =
            if (d > 0.0) n / d else null

        fun flushIfReady(nextBucket: Long) {
            if (curBucket == Long.MIN_VALUE) {
                curBucket = nextBucket; return
            }
            if (nextBucket != curBucket) {
                val start = curBucket * windowMs
                val end = (curBucket + 1) * windowMs

                val buyShare = safeDiv(tb, tb + ts) ?: 0.0
                val logRatio = if (tb > 0.0 && ts > 0.0) kotlin.math.ln(tb / ts) else 0.0
                val vwap = safeDiv(vwapNum, vwapDen)

                val w = WindowAgg(start, end, tb, ts, buyShare, logRatio, vwap, spread, obi, mid)

                trySend(w)

                // Write to Parquet but don't let it cancel the producer
                try {
                    if (System.currentTimeMillis() - currRotateMs > ROTATE_PERIOD_MS) {
                      rotateParq()
                      currRotateMs = System.currentTimeMillis()
                    }
                    saveRecord(w) // move to processRecord() & add Mutex
                } catch (t: Throwable) { 
                    log.error("Parquet write failed: ${t.message}", t)
                }

                curBucket = nextBucket
                tb = 0.0; ts = 0.0
                vwapNum = 0.0; vwapDen = 0.0
                spread = null; obi = null; mid = null
            }
        }

        val supervisor = SupervisorJob(coroutineContext.job)
        scope = this + supervisor
        val scoped = scope!!

        val jobBook = scoped.launch {
            orderBook.observeStream().collect { book ->
                val b = book.t / windowMs
                flushIfReady(b)
                spread = book.spread
                obi = book.obi
                mid = book.mid
            }
        }

        val jobTrades = scoped.launch {
            aggTrade.observeStream()
                .collect { tradeWrapper ->
                    if (tradeWrapper.aggTrade != null) {
                        var trade = tradeWrapper.aggTrade!!
                        val b = trade.E / windowMs
                        flushIfReady(b)

                        val qty = trade.q.toDoubleOrNull() ?: 0.0
                        val px = trade.p.toDoubleOrNull() ?: 0.0

                        if (trade.m) ts += qty else tb += qty
                        if (qty > 0.0) {
                            vwapNum += px * qty
                            vwapDen += qty
                        }
                    } else if (tradeWrapper.aggError != null) {
                        throw IllegalStateException(tradeWrapper.aggError!!.msg)
                    }
                }
        }


        awaitClose {
            jobTrades.cancel()
            jobBook.cancel()
            supervisor.cancel()
        }
    }

    fun rotateParq() {
      val output: OutputFile = LocalOutputFile(Paths.get("/tmp/tradesignal_${System.currentTimeMillis()}.parquet"))
      writer.close()
      writer = AvroParquetWriter.builder<GenericRecord>(output)
          .withSchema(AGG_WINDOW_SCHEMA)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .build()
    }

    fun stop() {
      writer.close()
      scope!!.cancel()
    }

    fun saveRecord(w: WindowAgg) {
        val record = GenericRecordBuilder(AGG_WINDOW_SCHEMA)
            .set("windowStart", w.windowStart)
            .set("windowEnd", w.windowEnd)
            .set("tb", w.tb)
            .set("ts", w.ts)
            .set("buyShare", w.buyShare)
            .set("logRatio", w.logRatio)
            .set("vwap", w.vwap)
            .set("spread", w.spread)
            .set("obi", w.obi)
            .set("mid", w.mid)
            .build()
        writer.write(record)
        log.info("Writing record: ${w.windowStart}")
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
