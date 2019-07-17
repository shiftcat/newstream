package com.example.stream.utils

import com.example.stream.types.db._
import com.example.stream.utils.Utils.{tokenize, topWords}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.util.parsing.json.JSON

object Converter {

  def toRow(jsonMap: Map[String, Any]) = {
    val testArti = jsonMap.get("articleId")

    val articleId = testArti match {
      case Some(e: Map[String, Any]) => Row(e.getOrElse("id", ""), e.getOrElse("press", ""))
      //case None => ArticleId("0", "unknow")
    }

    Row(
      articleId,
      jsonMap.getOrElse("subject", "").asInstanceOf[String],
      jsonMap.getOrElse("content", "").asInstanceOf[String],
      jsonMap.getOrElse("cate", "").asInstanceOf[String],
      jsonMap.getOrElse("writer", "").asInstanceOf[String],
      jsonMap.getOrElse("artiDate", "").asInstanceOf[String],
      jsonMap.getOrElse("artiTime", "").asInstanceOf[String]
    )
  }


  def rddToList(rdd: RDD[String]): List[Row] = {
    rdd
      .map(JSON.parseFull(_))
      .map {
        case Some(e: Map[String, Any]) => Converter.toRow(e)
        case None => Row()
      }
      .filter(m => {
        m.size > 0 && !m.getString(1).isEmpty
      })
      .map(r => {
        val content = r.getString(2)
        println("내용 : " + content)

        val tokenizedWords = tokenize(String.valueOf(content))
        println("tokenize : " + tokenizedWords.mkString(", "))

        val top10words = topWords(tokenizedWords, 10)
        println("top words => " + top10words)

        Row.fromSeq(r.toSeq ++ Seq(tokenizedWords, top10words.map(w => Row(w._1, w._2))))
      })
      .collect()
      .toList
  }


  import scala.language.postfixOps

  class int(value: Int) {

    var mValue = value

    def ++(): Int = {
      mValue += 1
      return mValue
    }
  }


  def toArticleTfIdfDbType(row: Row) = {
    val artiStruct = row.getStruct(0)

    var idx = new int(0)

    ArticleTfIdfDBType (
      ArticleId(artiStruct.getString(0), artiStruct.getString(1)),
      row.getString( idx++ ), // subject
      row.getString( idx++ ), // content

      row.getString( idx++ ), // cate
      row.getString( idx++ ), // writer

      row.getString( idx++ ), // artiDate
      row.getString( idx++ ), // artiTime

      row.getSeq[String]( idx++ ), // tokenize
      row.getSeq[Row]( idx++ ).map {
        case Row(str: String, cnt: Int) => WordCount(str, cnt)
      },

      row.get( idx++ ) match {
        case SparseVector(val1: Int, val2: Array[Int], val3: Array[Double] ) =>
          SparseVectorDBType(val1, val2.toSeq, val3.toSeq)
        case _ => SparseVectorDBType(0, Seq(), Seq())
      },
      row.get( idx++ ) match {
        case SparseVector(val1: Int, val2: Array[Int], val3: Array[Double] ) =>
          SparseVectorDBType(val1, val2.toSeq, val3.toSeq)
        case _ => SparseVectorDBType(0, Seq(), Seq())
      }

//      row.get( idx++ ) match {
//        case DenseVector(values:Array[Double]) => values.toSeq
//        case _ => Seq()
//      },

    )
  }


  def toArticleTokenizeDBType(row: Row) = {
    val artiStruct = row.getStruct(0)

    var idx = new int(0)

    ArticleTokenizeDBType (
      ArticleId(artiStruct.getString(0), artiStruct.getString(1)),
      row.getString( idx++ ), // subject
      row.getString( idx++ ), // content

      row.getString( idx++ ), // cate
      row.getString( idx++ ), // writer

      row.getString( idx++ ), // artiDate
      row.getString( idx++ ), // artiTime

      row.getSeq[String]( idx++ ), // tokenize
      row.getSeq[Row]( idx++ ).map {
        case Row(str: String, cnt: Int) => WordCount(str, cnt)
      }
    )
  }

}
