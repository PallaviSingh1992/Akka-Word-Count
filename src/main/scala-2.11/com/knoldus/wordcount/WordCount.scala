package com.knoldus.wordcount

import akka.actor.{ActorSystem, Props, Actor}
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask


case class ProcessString(str:String)
case class ProcessedResult(wordMap:Map[String,Int])


class StringCounterActor(filename:String) extends Actor{
  def receive={
    case "ProcessFile"=>{
                          println("Inside SCA")
                          import scala.io.Source._
                          fromFile(filename).getLines.foreach { lineOfFile =>
                                     context.actorOf(Props[WordCounterActor]) ! ProcessString(lineOfFile)}

                        }
      case _ => println("Oops File not found")
  }
}

class WordCounterActor extends Actor{
  def receive = {
    case ProcessString(str)=>{
      println("Inside WCA")
      val result:scala.collection.immutable.Map[String,Int]= str.split(" ").toList.groupBy(word=>word).map{case(key,value)=>(key,value.length)}
      context.actorOf(Props(new AggregateCounterActor)) ! ProcessedResult(result)
    }
    case _ =>{
      context.actorOf(Props(new AggregateCounterActor)) ! "ProcessedResult"
    }

  }
}



class AggregateCounterActor extends Actor{

  var wordsCountMap:Map[String,Int] = Map()
  def receive={
    case ProcessedResult(wordsMap)=>{
      println("Inside ACA")
    wordsCountMap=WordCount.merge(wordsCountMap,wordsMap)
      println(wordsCountMap)
    }
    case _ =>{
      println(wordsCountMap)
    }
  }
}


object WordCount {

  def main(args: Array[String]) {
    val system = ActorSystem("System")
    val filename="/home/knoldus-pallavi/Desktop/Commands"
    val fileActor = system.actorOf(Props(new StringCounterActor(filename)))
    implicit val timeout = Timeout(25 seconds)
    fileActor ! "ProcessFile"
  }

  def merge(m1:Map[String,Int],m2:Map[String,Int]):Map[String,Int]={
    m1 ++ m2.map { case (key, value) => key -> (value + m1.getOrElse(key, 0)) }
  }

}
