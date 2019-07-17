

package com.example.stream.types {

  package db {
    case class ArticleId(id: String, press: String)


    abstract class Article(
                            articleId: ArticleId,
                            subject: String,
                            content: String,
                            cate: String,
                            writer: String,
                            artiDate: String,
                            artiTime: String
                          )


    case class WordCount(word: String, count: Int)


    case class SparseVectorDBType(size: Int, indices: Seq[Int], values: Seq[Double])


    case class ArticleTokenizeDBType(
                                      articleId: ArticleId,
                                      subject: String,
                                      content: String,
                                      cate: String,
                                      writer: String,
                                      artiDate: String,
                                      artiTime: String,
                                      tokenize: Seq[String],
                                      topwords: Seq[WordCount]
                                    ) extends Article(articleId, subject, content, cate, writer, artiDate, artiTime)


    case class ArticleTfIdfDBType(
                                   articleId: ArticleId,
                                   subject: String,
                                   content: String,
                                   cate: String,
                                   writer: String,
                                   artiDate: String,
                                   artiTime: String,
                                   tokenize: Seq[String],
                                   topwords: Seq[WordCount],
                                   termFreqs: SparseVectorDBType,
                                   tfidfVec: SparseVectorDBType
                                   //  label: Double,
                                   //  rawPrediction: Seq[Double],
                                   //  probability: Seq[Double],
                                   //  prediction: Double
                                 ) extends Article(articleId, subject, content, cate, writer, artiDate, artiTime)
  }

  package struct {

    import org.apache.spark.sql.types.{StringType, StructType}

    object Schema {

      def jsonSchem = new StructType()
        .add("articleId", new StructType()
          .add("id", StringType)
          .add("press", StringType))
        .add("subject", StringType)
        .add("content", StringType)
        .add("cate", StringType)
        .add("writer", StringType)
        .add("artiDate", StringType)
        .add("artiTime", StringType)
    }
  }
}






