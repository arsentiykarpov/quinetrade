package com.quintrade.sources.binance.stream

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.callbackFlow
import io.ktor.client.HttpClient
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import org.slf4j.LoggerFactory
import com.quinetrade.NetClient

class WebSocketDataStream(val url: String): StreamRepository<String> {

    val client: HttpClient = NetClient().client

  override fun stream(): Flow<String> {
    return callbackFlow {
      client.webSocket(url) {
            for (frame in incoming) {
                frame as? Frame.Text ?: continue
                trySend(frame.readText())
            }
      }
    }
  }

}
