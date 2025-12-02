import kotlinx.serialization.encodeToString
import com.quintrade.sources.binance.data.AggTrade
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.nio.file.Path

private val json = Json {
    prettyPrint = false
    encodeDefaults = true
}

fun convertAggTradesCsvToJsonLines(
    input: Path,
    output: Path,
    symbol: String
) {
    Files.newBufferedReader(input).use { reader ->
        Files.newBufferedWriter(output).use { writer ->
            reader.lineSequence()
                .filter { it.isNotBlank() }
                .forEach { line ->
                    if (!line[0].isDigit()) return@forEach

                    val parts = line.split(',')
                    require(parts.size >= 8) {
                        "Unexpected CSV format: <$line>"
                    }

                    val rawTs = parts[5].trim().toLong()
                    val tradeTimeMs =
                        if (rawTs > 2_000_000_000_000L) rawTs / 1000 else rawTs

                    val dto = AggTrade.AggTradeDto(
                        e = "aggTrade",
                        E = tradeTimeMs,
                        s = symbol,
                        a = parts[0].trim().toLong(),
                        p = parts[1].trim(),
                        q = parts[2].trim(),
                        f = parts[3].trim().toLong(),
                        l = parts[4].trim().toLong(),
                        T = tradeTimeMs,
                        m = parts[6].trim().toBoolean(),
                        M = parts[7].trim().toBoolean()
                    )

                    val jsonLine = json.encodeToString(dto)
                    writer.appendLine(jsonLine)
                }
        }
    }
}
