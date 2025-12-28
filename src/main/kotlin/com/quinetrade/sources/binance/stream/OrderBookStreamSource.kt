package com.quintrade.sources.binance.stream

import com.quinetrade.sources.binance.data.DepthDiff
import com.quinetrade.sources.binance.stream.BaseStreamSource
import com.quinetrade.sources.binance.stream.OrderBookL2
import kotlinx.serialization.json.Json
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.io.OutputFile
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import java.nio.file.Paths

class OrderBookStreamSource() : BaseStreamSource<OrderBookStreamSource.BookStats>(
    BookStats::class,
    "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms"
) {

    var lastUpdateId: Long = 0 // выставьте после snapshot
    val book = OrderBookL2()

    val ORDERBOOK_SCHEMA: Schema = Schema.Parser().parse(
        """
          {
            "type":"record","name":"OrderBook","namespace":"btc",
            "fields":[
              {"name":"e","type":"long"}
            ]
          }
          """.trimIndent()
    )
    val output: OutputFile = LocalOutputFile(Paths.get("/tmp/orderbook.parquet"))
    val json = Json { ignoreUnknownKeys = true }
    val writer = AvroParquetWriter.builder<GenericRecord>(output)
        .withSchema(ORDERBOOK_SCHEMA)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()


    override fun getStreamRepository(): StreamRepository<String> {
        return WebSocketDataStream(url)
    }

    override fun parseText(line: String): BookStats {
        val json = Json { ignoreUnknownKeys = true }
        val diff = json.decodeFromString<DepthDiff>(line)

        if (diff.prevFinalUpdateId != null && diff.prevFinalUpdateId != lastUpdateId) {
            throw RuntimeException("GAP detected, need resync")
        }

        diff.b.forEach { (p, q) -> book.apply('b', p.toDouble(), q.toDouble()) }
        diff.a.forEach { (p, q) -> book.apply('a', p.toDouble(), q.toDouble()) }
        lastUpdateId = diff.finalUpdateId
        val spread = book.spreadAbs()
        val mid = book.mid()
        val obi = book.obi(10)
        return BookStats(diff.eventTime!!, spread!!, obi!!, mid!!, null)
    }


    override fun error(): BookStats {
        return BookStats(0, 0.0, 0.0, 0.0, "error")
    }

    data class BookStats(
        val t: Long,
        val spread: Double,
        val obi: Double,
        val mid: Double,
        var error: String?
    )
}
