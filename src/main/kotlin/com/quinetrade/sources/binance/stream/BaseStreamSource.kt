package com.quinetrade.sources.binance.stream

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.coroutineScope
import io.ktor.websocket.Frame
import io.ktor.client.HttpClient
import com.quinetrade.NetClient
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.websocket.readText
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.slf4j.LoggerFactory
import kotlin.reflect.KClass


abstract class BaseStreamSource<T : Any>(
  final override val type: KClass<T>,
  private var url: String) : StreamSource<T> {

    val client: HttpClient = NetClient().client
    private val log = LoggerFactory.getLogger("BinanceWS")
    val stream = MutableSharedFlow<T>()

    suspend fun poll() = coroutineScope {
        client.webSocket(url) {
            // Примерно раз в N секунд можно логировать агрегаты
            launch {
                while (isActive) {
                    delay(1000)
                }
            }

            for (frame in incoming) {
                frame as? Frame.Text ?: continue
                stream.emit(parseFrame(frame))
                //log("<<< ${frame.readText()}")
            }
        }
    }

    protected fun log(text: String) {
      log.info(text)
    }

    private fun parseFrame(frame: Frame): T {
        var frameText = frame as? Frame.Text ?: return error()
        return parseTextFrame(frameText)
    }

    abstract fun parseTextFrame(frame: Frame.Text): T

    abstract fun error(): T

    override fun observeStream(): Flow<T> {
        return stream
    }

} 
