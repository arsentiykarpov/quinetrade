package com.quinetrade.sources.binance.stream

import com.quinetrade.sources.binance.data.*
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import kotlinx.coroutines.*
import kotlinx.serialization.*
import kotlinx.serialization.json.*
import io.ktor.websocket.*
import org.slf4j.LoggerFactory


class BnsOrderBookPoll(val client: HttpClient) {

  private val log = LoggerFactory.getLogger("BinanceWS")

  suspend fun poll() = coroutineScope {  
    val symbol = "btcusdt"
    val book = OrderBookL2()
    val json = Json { ignoreUnknownKeys = true }
    var lastUpdateId: Long = 0 // выставьте после snapshot

    // 2) Подписка на diffs
    val url = "wss://stream.binance.com:9443/ws/$symbol@depth@100ms"
    client.webSocket(url) {
        // Примерно раз в N секунд можно логировать агрегаты
        launch {
            while (isActive) {
                delay(1000)
                val spread = book.spreadAbs()
                val mid = book.mid()
                val obi = book.obi(10)
                if (mid != null && spread != null && obi != null) {
                    println("mid=$mid spread=$spread obi=$obi")
                }
            }
        }

        for (frame in incoming) {
            frame as? Frame.Text ?: continue
            log.info("<<< ${frame.readText()}")
            val diff = json.decodeFromString<DepthDiff>(frame.readText())

            // Binance правила синхронизации:
            // Пропускаем всё до первого diff, где diff.finalUpdateId >= snapshotLastUpdateId+1
            if (lastUpdateId == 0L) continue // ждём, пока подгрузите snapshot и установите lastUpdateId

            // Проверяем непрерывность
            if (diff.prevFinalUpdateId != null && diff.prevFinalUpdateId != lastUpdateId) {
                // рассинхрон → заново получить snapshot, обновить lastUpdateId и continue
                println("GAP detected, need resync")
                continue
            }

            // Применяем изменения
            diff.b.forEach { (p, q) -> book.apply('b', p.toDouble(), q.toDouble()) }
            diff.a.forEach { (p, q) -> book.apply('a', p.toDouble(), q.toDouble()) }
            lastUpdateId = diff.finalUpdateId
        }
    }  
}
}
