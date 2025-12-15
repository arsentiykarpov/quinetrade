package com.quinetrade.sources.binance.stream

import com.quintrade.sources.binance.data.AggTrade
import com.quintrade.sources.binance.data.AGGTRADE_SCHEMA
import com.quintrade.sources.binance.data.toAvroRecord
import com.quintrade.sources.binance.stream.LocalOutputFile
import com.quintrade.sources.binance.stream.StreamRepository
import com.quintrade.sources.binance.stream.WebSocketDataStream
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.serialization.json.Json
import org.apache.parquet.io.OutputFile
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.avro.generic.GenericRecord
import java.nio.file.Paths

class AggTradeStreamSource() : BaseStreamSource<AggTrade>(
    AggTrade::class,
    "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
) {

    val output: OutputFile = LocalOutputFile(Paths.get("/tmp/aggtrades.parquet"))
    val json = Json { ignoreUnknownKeys = true }
    val writer = AvroParquetWriter.builder<GenericRecord>(output)
        .withSchema(AGGTRADE_SCHEMA)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()

    override fun getStreamRepository(): StreamRepository<String> {
       return WebSocketDataStream(url)
    }

    override fun parseText(line: String): AggTrade {
        val trade = json.decodeFromString<AggTrade.AggTradeDto>(line)
        trade.aggTrade = trade
        writer.write(trade.toAvroRecord())
        return trade
    }

    init {

    }


    override fun error(): AggTrade {
        val err = AggTrade.AggTradeError("agg trade error")
        err.aggError = err
        return err
    }
} 
