package com.example.stream.utils

import java.sql.{Date, Timestamp}

import com.mongodb.spark.exceptions.MongoTypeConversionException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.bson._
import org.bson.types.Decimal128
import collection.JavaConverters._

import scala.util.{Failure, Success, Try}

object BSonUtil {



  private def wrappedDataTypeToBsonValueMapper(elementType: DataType): (Any) => BsonValue = {
    element =>
      Try(dataTypeToBsonValueMapper(elementType)(element)) match {
        case Success(bsonValue)                        => bsonValue
        case Failure(ex: MongoTypeConversionException) => throw ex
        case Failure(e)                                => throw new MongoTypeConversionException(s"Cannot cast $element into a $elementType")
      }
  }


  def rowToDocument(row: Row): BsonDocument = rowToDocumentMapper(row.schema)(row)


  private def rowToDocumentMapper(schema: StructType): (Row) => BsonDocument = {
    // foreach field type, decide what function to use to map its value
    val mappers = schema.fields.map({ field =>
      if (field.dataType == NullType) {
        (data: Any, document: BsonDocument) => document.append(field.name, new BsonNull())
      } else {
        val mapper = wrappedDataTypeToBsonValueMapper(field.dataType)
        (data: Any, document: BsonDocument) => if (data != null) document.append(field.name, mapper(data))
      }
    })

    (row: Row) => {
      val document = new BsonDocument()

      // foreach field of the row, add it to the BsonDocument by mapping with corresponding mapper
      mappers.zipWithIndex.foreach({
        case (mapper, i) =>
          val value = if (row.isNullAt(i)) null else row.get(i)
          mapper(value, document)
      })

      document
    }
  }


  private def structTypeToBsonValueMapper(dataType: StructType): (Row) => BsonValue = {
    dataType match {
      case BsonCompatibility.ObjectId()            => BsonCompatibility.ObjectId.apply
      case BsonCompatibility.MinKey()              => BsonCompatibility.MinKey.apply
      case BsonCompatibility.MaxKey()              => BsonCompatibility.MaxKey.apply
      case BsonCompatibility.Timestamp()           => BsonCompatibility.Timestamp.apply
      case BsonCompatibility.JavaScript()          => BsonCompatibility.JavaScript.apply
      case BsonCompatibility.JavaScriptWithScope() => BsonCompatibility.JavaScriptWithScope.apply
      case BsonCompatibility.RegularExpression()   => BsonCompatibility.RegularExpression.apply
      case BsonCompatibility.Undefined()           => BsonCompatibility.Undefined.apply
      case BsonCompatibility.Binary()              => BsonCompatibility.Binary.apply
      case BsonCompatibility.Symbol()              => BsonCompatibility.Symbol.apply
      case BsonCompatibility.DbPointer()           => BsonCompatibility.DbPointer.apply
      case _                                       => rowToDocumentMapper(dataType)
    }
  }



  private def arrayTypeToBsonValueMapper(elementType: DataType): (Seq[Any]) => BsonValue = {
    val bsonArrayValuesMapper = elementType match {
      case subDocuments: StructType => {
        val mapper = structTypeToBsonValueMapper(subDocuments)
        (data: Seq[Any]) => data.map(x => mapper(x.asInstanceOf[Row])).asJava
      }
      case subArray: ArrayType => {
        val mapper = arrayTypeToBsonValueMapper(subArray.elementType)
        (data: Seq[Any]) => data.map(x => mapper(x.asInstanceOf[Seq[Any]])).asJava
      }
      case _ => {
        val mapper = wrappedDataTypeToBsonValueMapper(elementType)
        (data: Seq[Any]) => data.map(x => mapper(x)).asJava
      }
    }
    data => new BsonArray(bsonArrayValuesMapper(data))
  }



  private def mapTypeToBsonValueMapper(valueType: DataType): (Map[String, Any]) => BsonValue = {
    val internalDataMapper = valueType match {
      case subDocuments: StructType => {
        val mapper = structTypeToBsonValueMapper(subDocuments)
        (data: Map[String, Any]) => data.map(kv => new BsonElement(kv._1, mapper(kv._2.asInstanceOf[Row])))
      }
      case subArray: ArrayType => {
        val mapper = arrayTypeToBsonValueMapper(subArray.elementType)
        (data: Map[String, Any]) => data.map(kv => new BsonElement(kv._1, mapper(kv._2.asInstanceOf[Seq[Any]])))
      }
      case _ => {
        val mapper = wrappedDataTypeToBsonValueMapper(valueType)
        (data: Map[String, Any]) => data.map(kv => new BsonElement(kv._1, mapper(kv._2)))
      }
    }

    data => new BsonDocument(internalDataMapper(data).toList.asJava)
  }


  private def dataTypeToBsonValueMapper(elementType: DataType): (Any) => BsonValue = {
    elementType match {
      case BinaryType => (element: Any) => new BsonBinary(element.asInstanceOf[Array[Byte]])
      case BooleanType => (element: Any) => new BsonBoolean(element.asInstanceOf[Boolean])
      case DateType => (element: Any) => new BsonDateTime(element.asInstanceOf[Date].getTime)
      case DoubleType => (element: Any) => new BsonDouble(element.asInstanceOf[Double])
      case IntegerType => (element: Any) => new BsonInt32(element.asInstanceOf[Int])
      case LongType => (element: Any) => new BsonInt64(element.asInstanceOf[Long])
      case StringType => (element: Any) => new BsonString(element.asInstanceOf[String])
      case TimestampType => (element: Any) => new BsonDateTime(element.asInstanceOf[Timestamp].getTime)
      case arrayType: ArrayType => {
        val mapper = arrayTypeToBsonValueMapper(arrayType.elementType)
        (element: Any) => mapper(element.asInstanceOf[Seq[_]])
      }
      case schema: StructType => {
        val mapper = structTypeToBsonValueMapper(schema)
        (element: Any) => mapper(element.asInstanceOf[Row])
      }
      case mapType: MapType =>
        mapType.keyType match {
          case StringType => element => mapTypeToBsonValueMapper(mapType.valueType)(element.asInstanceOf[Map[String, _]])
          case _ => element => throw new MongoTypeConversionException(
            s"Cannot cast $element into a BsonValue. MapTypes must have keys of StringType for conversion into a BsonDocument"
          )
        }
      case _ if elementType.typeName.startsWith("decimal") =>
        val jBigDecimal = (element: Any) => element match {
          case jDecimal: java.math.BigDecimal => jDecimal
          case _                              => element.asInstanceOf[BigDecimal].bigDecimal
        }
        (element: Any) => new BsonDecimal128(new Decimal128(jBigDecimal(element)))
      case _ =>
        (element: Any) => throw new MongoTypeConversionException(s"Cannot cast $element into a BsonValue. $elementType has no matching BsonValue.")
    }
  }


}
