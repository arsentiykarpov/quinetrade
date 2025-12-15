package com.quinetrade.sources.binance.stream

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.coroutineScope
import io.ktor.websocket.Frame
import io.ktor.client.HttpClient
import com.quinetrade.NetClient
import com.quintrade.sources.binance.stream.StreamRepository
import com.quintrade.sources.binance.stream.CsvStreamDataSource
import com.quintrade.sources.binance.stream.WebSocketDataStream
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.websocket.readText
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import org.slf4j.LoggerFactory
import kotlin.reflect.KClass


abstract class BaseStreamSource<T : Any>(
  final override val type: KClass<T>,
  var url: String) : StreamSource<T> {

    val client: HttpClient = NetClient().client
    var log = LoggerFactory.getLogger(javaClass.simpleName)
    val stream = MutableSharedFlow<T>()
    var pollJob: Job? = null
    var streamRepo: StreamRepository<String> = CsvStreamDataSource()

    fun poll(scope: CoroutineScope) {
        pollJob = scope.launch {
        getStreamRepository().stream().collect{
          log.info(it)
          stream.emit(parseText(it))
        }
      }
    }

    protected fun log(text: String) {
      log.info(text)
    }

    abstract fun getStreamRepository(): StreamRepository<String> 

    abstract fun parseText(line: String): T

    abstract fun error(): T

    override fun observeStream(): Flow<T> {
        return stream
    }

    override fun clear() {
      pollJob!!.cancel()
    }
} 
