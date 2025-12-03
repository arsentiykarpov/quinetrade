package com.quintrade.sources.binance.stream

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.flow
import convertAggTradesCsvToJsonLines
import java.nio.file.Paths

class CsvStreamDataSource(): StreamRepository<String> {

      val rootPath = Paths.get("").toAbsolutePath()

    override fun stream(): Flow<String> { 
      return convertAggTradesCsvToJsonLines(rootPath.resolve("btcusdt.csv"), rootPath.resolve("btcusdt_output.json"), "BTCUSDT") 
    }

}
