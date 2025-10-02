package com.quintrade.sources.binance.stream

import com.quinetrade.sources.binance.stream.BaseStreamSource
import io.ktor.websocket.Frame
import io.ktor.websocket.readText

class SymbolStreamSource(): BaseStreamSource<String>("wss://stream.binance.com:9443/ws/btcusdt@depth@100ms") {

    override fun parseTextFrame(frame: Frame.Text): String { return frame.readText()}

    override fun error(): String { return "error"}

}
