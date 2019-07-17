package com.example.stream.utils

import org.bitbucket.eunjeon.seunjeon.Analyzer

object Utils {


  def wordCount(words: Seq[String]) = {
    words
      .map((_, 1))
      .groupBy(_._1)
      .map(e => (e._1, e._2.map(_._2).reduce(_+_)))
      .toList
      .sortWith(_._2 > _._2)
  }


  def tokenize(content: String) = {
    Analyzer.parse(content)
      .flatMap(_.deCompound())
      .map(ln => {
        val word = ln.morpheme.surface
        val ft = ln.morpheme.feature(0)
        (word, ft)
      })
      .filter(tu => "NNG,NNP,SN,SL,SH,XPN".contains(tu._2))
      .map(_._1)
      .toList
  }


  def topWords(words: List[String], n: Int) = {
    words
      .map((_, 1))
      .groupBy(_._1)
      .map(e => (e._1, e._2.map(_._2).reduce(_ + _)))
      .toList
      .sortWith(_._2 > _._2)
      .take(n)
  }


}
