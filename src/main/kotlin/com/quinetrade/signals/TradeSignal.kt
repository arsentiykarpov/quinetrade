package com.quintrade.signals

import com.quintrade.sources.binance.data.AggTrade
import com.quintrade.sources.binance.stream.AggTradeStreamSource
import com.quintrade.sources.binance.stream.OrderBookStreamSource
import com.quintrade.sources.binance.stream.LocalOutputFile
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
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

    val output: OutputFile = LocalOutputFile(Paths.get("/tmp/tradesignal.parquet"))
    val json = Json { ignoreUnknownKeys = true }
    val writer = AvroParquetWriter.builder<GenericRecord>(output)
        .withSchema(AGG_WINDOW_SCHEMA)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()

    private val buf = ArrayDeque<Double>() // buyShare history
    private var cvd = 0.0
    private val zLen = 60

    suspend fun process() = coroutineScope {
        aggWindows().collect { w ->
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
            val zBuy = if (buf.size >= 10) (buyShare - mean) / std else 0.0
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
                log.debug(book.toString())
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
        aggTrade.poll()
        orderBook.poll()
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
