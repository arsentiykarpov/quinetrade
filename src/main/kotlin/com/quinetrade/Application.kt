package com.quinetrade

import com.quinetrade.sources.binance.stream.AggTradeStreamSource
import com.quinetrade.signals.WhalesSignal
import com.quinetrade.signals.WHALE_SCHEMA
import com.quintrade.signals.TradeSignal
import com.quintrade.sources.binance.stream.OrderBookStreamSource
import io.ktor.server.application.*
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import org.apache.parquet.io.OutputFile
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.avro.generic.GenericRecord
import com.quintrade.sources.binance.stream.LocalOutputFile
import java.nio.file.Paths

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(args)
}

fun Application.initClient() {
    val log = LoggerFactory.getLogger("App logger")
    var spreadStream = OrderBookStreamSource()
    var tradeStream = AggTradeStreamSource()
    var tradeSignals: TradeSignal = TradeSignal(spreadStream, tradeStream, 5_000, log)

    val output: OutputFile = LocalOutputFile(Paths.get("/tmp/whalesignal_${System.currentTimeMillis()}.parquet"))
    val json = Json { ignoreUnknownKeys = true }
    var writer = AvroParquetWriter.builder<GenericRecord>(output)
       .withSchema(WHALE_SCHEMA)
       .withCompressionCodec(CompressionCodecName.SNAPPY)
       .build()

    val handler = CoroutineExceptionHandler { _, e -> log.error("Coroutine error", e) }
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO + handler)

    var whaleSignal: WhalesSignal = WhalesSignal(tradeStream, scope)

    spreadStream.poll(scope)
    tradeStream.poll(scope)

    scope.launch {
        try {
            tradeSignals.aggWindows().collect {
            }
        } catch (e: Exception) {
        } finally {
            tradeSignals.stop()
            scope.cancel()
        }
    }

    scope.launch {
        try {
            whaleSignal.observe().collect {
              log.info("Whale detected: ${it}")
            }
        } catch (e: Exception) {
        } finally {
            //whaleSignal.stop()//TODO:
            //scope.cancel()
        }
    }
}

fun Application.module() {
    configureSockets()
    configureSerialization()
    configureMonitoring()
    configureSecurity()
    configureHTTP()
    initClient()
    //configureRouting()
}
