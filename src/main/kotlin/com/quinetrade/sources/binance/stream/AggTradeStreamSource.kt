package com.quintrade.sources.binance.stream

import com.quinetrade.sources.binance.stream.BaseStreamSource
import com.quintrade.sources.binance.data.AggTrade
import com.quintrade.sources.binance.data.AGGTRADE_SCHEMA
import com.quintrade.sources.binance.data.toAvroRecord
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.serialization.json.Json
import org.apache.parquet.io.OutputFile
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import java.nio.file.Paths

class AggTradeStreamSource(): BaseStreamSource<AggTrade>("wss://stream.binance.com:9443/ws/btcusdt@aggTrade") {

    val output: OutputFile = LocalOutputFile(Paths.get("/tmp/aggtrades.parquet"))
    val json = Json { ignoreUnknownKeys = true }
    val writer = AvroParquetWriter.builder<GenericRecord>(output)
                  .withSchema(AGGTRADE_SCHEMA)
                  .withCompressionCodec(CompressionCodecName.SNAPPY)
                  .build()
    init {

    }

    override fun parseTextFrame(frame: Frame.Text): AggTrade { 
      val trade = json.decodeFromString<AggTrade.AggTradeDto>(frame.readText())
      writer.write(trade.toAvroRecord())
      return trade
    }

    override fun error(): AggTrade {
      return AggTrade.AggTradeError("error")
    }
    
} 
