package com.quintrade.sources.binance.data

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import kotlinx.serialization.SerialName

sealed class AggTrade {

  var aggTrade: AggTradeDto? = null
  var aggError: AggTradeError? = null

    @kotlinx.serialization.Serializable
    data class AggTradeDto(
        val e: String,  // "aggTrade"
        val E: Long,    // event time ms
        val s: String,  // symbol
        val a: Long,    // aggregate trade id
        val p: String,  // price as string
        val q: String,  // qty as string
        val f: Long,    // first trade id
        val l: Long,    // last trade id
        val T: Long,    // trade time ms
        @SerialName("m")
        @get:JvmName("isBuyerMakerFlag")
        val m: Boolean, // buyer is maker
        @get:JvmName("getIgnoreFlag")
        @SerialName("M")
        val M: Boolean  // ignore
    ) : AggTrade()

    data class AggTradeError(val msg: String) : AggTrade()

}

enum class TakerSide { BUY, SELL }

val AggTrade.AggTradeDto.takerSide: TakerSide
    get() = if (m) TakerSide.SELL else TakerSide.BUY

val AggTrade.AggTradeDto.priceD: Double? get() = p.toDoubleOrNull()
val AggTrade.AggTradeDto.qtyD: Double? get() = q.toDoubleOrNull()

fun AggTrade.AggTradeDto.toTakerDelta(): Pair<Double, Double> {
    val qty = q.toDouble()
    return if (m) 0.0 to qty else qty to 0.0
    // m == true -> buyer is maker -> taker is seller -> taker sell
    // m == false -> buyer is taker -> taker buy
}

fun AggTrade.AggTradeDto.toAvroRecord(): GenericRecord =
    GenericData.Record(AGGTRADE_SCHEMA).apply {
        put("e", e)
        put("E", E)
        put("s", s)
        put("a", a)
        put("p", p)
        put("q", q)
        put("p_d", p.toDoubleOrNull())
        put("q_d", q.toDoubleOrNull())
        put("f", f)
        put("l", l)
        put("T", T)
        put("m", m) // see note below about rename
        put("M", true)         // or drop field & schema col if unused
    }
val AGGTRADE_SCHEMA: Schema = Schema.Parser().parse(
    """
    {
      "type":"record","name":"AggTrade","namespace":"btc",
      "fields":[
        {"name":"e","type":"string"},
        {"name":"E","type":"long"},
        {"name":"s","type":"string"},
        {"name":"a","type":"long"},
        {"name":"p","type":"string"},
        {"name":"q","type":"string"},
        {"name":"p_d","type":["null","double"],"default":null},
        {"name":"q_d","type":["null","double"],"default":null},
        {"name":"f","type":"long"},
        {"name":"l","type":"long"},
        {"name":"T","type":"long"},
        {"name":"m","type":"boolean"},
        {"name":"M","type":"boolean"}
      ]
    }
    """.trimIndent()
)
