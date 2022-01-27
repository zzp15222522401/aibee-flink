package com.aibee.flink.cdc

import com.ververica.cdc.debezium.DebeziumDeserializationSchema
import com.ververica.cdc.debezium.utils.TemporalConversions
import io.debezium.time._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import com.alibaba.fastjson.JSONObject
import java.sql
import java.time.{Instant, LocalDateTime, ZoneId}
import scala.collection.JavaConverters._

class StructDebeziumDeserializationSchema(serverTimeZone: String) extends DebeziumDeserializationSchema[String] {

  override def deserialize(sourceRecord: SourceRecord, collector: Collector[String]): Unit = {
    // 解析主键
    val key = sourceRecord.key().asInstanceOf[Struct]
    val keyJs = parseStruct(key)

    // 解析值
    val value = sourceRecord.value().asInstanceOf[Struct]
    val source = value.getStruct("source")
    val sourceStruct = parseStruct(source)
    val before = parseStruct(value.getStruct("before"))
    val after = parseStruct(value.getStruct("after"))
    var jsonBefore: JSONObject =new JSONObject
    var jsonAfter: JSONObject =new JSONObject
    var jsonSource: JSONObject =new JSONObject
    if(before !=null){
      getJSONObject(before,jsonBefore)
    }
    if(after !=null){
      getJSONObject(after,jsonAfter)
    }
    if(sourceStruct !=null){
      getJSONObject(sourceStruct,jsonSource)
    }
    val ts_ms =  LocalDateTime.ofInstant(Instant.ofEpochMilli(source.getInt64("ts_ms")), ZoneId.of(serverTimeZone)).toString
    var result :JSONObject = new JSONObject
    result.put("before",jsonBefore)
    result.put("after",jsonAfter)
    result.put("source",jsonSource)
    result.put("op",value.get("op"))
    result.put("ts_ms",value.get("ts_ms"))
    val transaction: AnyRef = value.get("transaction")
    result.put("transaction",if (transaction ==null) "" else transaction )
    collector.collect(result.toJSONString)
  }

  /** 解析[[Struct]]结构为json字符串 */
  private def parseStruct(struct: Struct): Map[String,Any] = {
    if (struct == null) return null
    val map = struct.schema().fields().asScala.map(field => {
      val v = struct.get(field)
      val typ = field.schema().name()
      //      println(s"$v, $typ, ${field.name()}")
      val value = v match {
        case long if long.isInstanceOf[Long] => convertLongToTime(long.asInstanceOf[Long], typ)
        case iv if iv.isInstanceOf[Int] => convertIntToDate(iv.asInstanceOf[Int], typ)
        case iv if iv == null => null
        case _ => convertObjToTime(v, typ)
      }
      (field.name(), value)
    }).toMap
    map
  }

  /** 类型转换 */
  private def convertObjToTime(obj: Any, typ: String): Any = {
    typ match {
      case Time.SCHEMA_NAME | MicroTime.SCHEMA_NAME | NanoTime.SCHEMA_NAME =>
        sql.Time.valueOf(TemporalConversions.toLocalTime(obj)).toString
      case Timestamp.SCHEMA_NAME | MicroTimestamp.SCHEMA_NAME | NanoTimestamp.SCHEMA_NAME | ZonedTimestamp.SCHEMA_NAME =>
        sql.Timestamp.valueOf(TemporalConversions.toLocalDateTime(obj, ZoneId.of("UTC+8"))).toString.dropRight(2)
      case _ => obj
    }
  }

  /** long 转换为时间类型 */
  private def convertLongToTime(obj: Long, typ: String): Any = {
    val time_schema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Time")
    val date_schema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Date")
    val timestamp_schema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Timestamp")
    typ match {
      case Time.SCHEMA_NAME =>
        org.apache.kafka.connect.data.Time.toLogical(time_schema, obj.asInstanceOf[Int]).toInstant.atZone(ZoneId.of(serverTimeZone)).toLocalTime.toString
      case MicroTime.SCHEMA_NAME =>
        org.apache.kafka.connect.data.Time.toLogical(time_schema, (obj / 1000).asInstanceOf[Int]).toInstant.atZone(ZoneId.of(serverTimeZone)).toLocalTime.toString
      case NanoTime.SCHEMA_NAME =>
        org.apache.kafka.connect.data.Time.toLogical(time_schema, (obj / 1000 / 1000).asInstanceOf[Int]).toInstant.atZone(ZoneId.of(serverTimeZone)).toLocalTime.toString
      case Timestamp.SCHEMA_NAME =>
        val t = org.apache.kafka.connect.data.Timestamp.toLogical(timestamp_schema, obj).toInstant.atZone(ZoneId.of(serverTimeZone)).toLocalDateTime
        java.sql.Timestamp.valueOf(t).toString.dropRight(2)
      case MicroTimestamp.SCHEMA_NAME =>
        val t = org.apache.kafka.connect.data.Timestamp.toLogical(timestamp_schema, obj / 1000).toInstant.atZone(ZoneId.of(serverTimeZone)).toLocalDateTime
        java.sql.Timestamp.valueOf(t).toString
      case NanoTimestamp.SCHEMA_NAME =>
        val t = org.apache.kafka.connect.data.Timestamp.toLogical(timestamp_schema, obj / 1000 / 1000).toInstant.atZone(ZoneId.of(serverTimeZone)).toLocalDateTime
        java.sql.Timestamp.valueOf(t).toString
      case Date.SCHEMA_NAME =>
        org.apache.kafka.connect.data.Date.toLogical(date_schema, obj.asInstanceOf[Int]).toInstant.atZone(ZoneId.of(serverTimeZone)).toLocalDate.toString
      case _ => obj
    }
  }

  private def convertIntToDate(obj: Int, typ: String): Any = {
    val date_schema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Date")
    typ match {
      case Date.SCHEMA_NAME =>
        org.apache.kafka.connect.data.Date.toLogical(date_schema, obj).toInstant.atZone(ZoneId.of(serverTimeZone)).toLocalDate.toString
      case _ => obj
    }
  }
  override def getProducedType: TypeInformation[String] = {
    TypeInformation.of(classOf[String])
  }
  private def getJSONObject(map: Map[String,Any],date: JSONObject): Unit ={
    for (elem <- map) {
      if(elem._2 == null){
        date.put(elem._1,"")
      }else{
        date.put(elem._1,elem._2)
      }
    }
  }

}



