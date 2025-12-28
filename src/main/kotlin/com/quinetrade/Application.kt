package com.quinetrade

import com.quinetrade.sources.binance.stream.AggTradeStreamSource
import com.quinetrade.sources.binance.stream.CsvStreamSource
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
import com.quintrade.trade.TradeStrategy
import java.nio.file.Paths

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(args)
}

fun Application.initClient() {
    val log = LoggerFactory.getLogger("App logger")
    val handler = CoroutineExceptionHandler { _, e -> log.error("Coroutine error", e) }
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO + handler)

    var spreadStream = OrderBookStreamSource()
    var tradeStream = AggTradeStreamSource()
    var csvStream = CsvStreamSource()
    var tradeSignals: TradeSignal = TradeSignal(spreadStream, tradeStream, 5_000, log, scope)
    var tradeStrategy = TradeStrategy(tradeSignals, scope, log)
    var whaleSignal: WhalesSignal = WhalesSignal(tradeStream, scope)

    spreadStream.poll(scope)
    tradeStream.poll(scope)
//    csvStream.poll(scope)

//    scope.launch {
//        try {
//            tradeSignals.observe().collect {
//            }
//        } catch (e: Exception) {
//        } finally {
//            tradeSignals.stop()
//            scope.cancel()
//        }
//    }

    scope.launch {
        try {
          tradeSignals.priceLogReturns().collect {
            log.debug(it.toString())
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
