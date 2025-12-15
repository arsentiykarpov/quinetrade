package com.quinetrade.sources.binance.stream

import com.quintrade.sources.binance.data.AggTrade
import com.quintrade.sources.binance.stream.CsvStreamDataSource
import com.quintrade.sources.binance.stream.StreamRepository
import kotlinx.serialization.json.Json

class CsvStreamSource() : BaseStreamSource<AggTrade>(
    AggTrade::class,
    ""
) {

    val json = Json { ignoreUnknownKeys = true }

    override fun getStreamRepository(): StreamRepository<String> {
       return CsvStreamDataSource()
    }

    override fun parseText(line: String): AggTrade {
        val trade = json.decodeFromString<AggTrade.AggTradeDto>(line)
        return trade
    }


    override fun error(): AggTrade {
        val err = AggTrade.AggTradeError("agg trade error")
        err.aggError = err
        return err
    }
} 
