package it.polimi.neo4jarm

import java.io.File

import weka.core.Instances
import weka.core.converters.ConverterUtils.DataSource
import weka.associations.Apriori

import scala.sys.process._



object ToPyt extends App{

  def firstLine(f: java.io.File): Option[String] = {
    val src = io.Source.fromFile(f)
    try {
      src.getLines.find(_ => true)
    } finally {
      src.close()
    }
  }

  def executePyConvertIntoNeo4J(): Unit ={

    val FILE_PATH = args(0)
    // val FILE_PATH = "/Users/abernasconi/Documents/neo4j-community-3.4.1/import/original/breve/"

    val flag_title = args(1)
    val TITLE_FILE_NAME = args(2)
    val TITLE_HEADER = firstLine(new File(FILE_PATH + TITLE_FILE_NAME + "_header.csv")).get

    //val TITLE_FILE_NAME = "title.basics"
    //val TITLE_FILE_NAME = "title.basics_breve"
    //val TITLE_HEADER = "tconst:ID,:LABEL,primaryTitle,originalTitle,isAdult:int,startYear,endYear,runtimeMinutes,:LABEL\n"

    val flag_name = args(3)
    val NAME_FILE_NAME = args(4)
    val NAME_HEADER = firstLine(new File(FILE_PATH + NAME_FILE_NAME + "_header.csv")).get

    //val NAME_FILE_NAME = "name.basics"
    //val NAME_FILE_NAME = "name.basics_breve"
    //val NAME_HEADER = "nconst:ID,primaryName,birthYear,deathYear,:LABEL\n"

    val flag_role = args(5)
    val ROLE_FILE_NAME = args(6)
    val ROLE_HEADER = firstLine(new File(FILE_PATH + ROLE_FILE_NAME + "_header.csv")).get
    //val ROLE_FILE_NAME = "title.principals"
    //val ROLE_FILE_NAME = "title.principals_breve"
    //val ROLE_HEADER = ":START_ID,ordering:int,:END_ID,:TYPE,job,characters:string[]\n"

    val num_cost = args(7)

    //val thisPath = getClass.getResource("").getPath.dropRight(1)
    //val pyScriptPath = thisPath.dropRight(thisPath.size - thisPath.lastIndexOf("/"))

    val pyScriptClass = args(8)

    val result = "python " +
      pyScriptClass + " " +
      FILE_PATH + " " +
      flag_title + " " +
      TITLE_FILE_NAME + " " +
      TITLE_HEADER + " " +
      flag_name + " " +
      NAME_FILE_NAME + " " +
      NAME_HEADER + " " +
      flag_role + " " +
      ROLE_FILE_NAME + " " +
      ROLE_HEADER + " " +
      num_cost !
  }

  def executePyQueryResultIntoMatrix() = ???

  def executePyRunWEKA(): Unit ={

    val source: DataSource = new DataSource("/Users/abernasconi/Downloads/matrix.arff")
    val data: Instances = source.getDataSet()
    if(data.classIndex() == -1)
      data.setClassIndex(data.numAttributes() - 1)

    if (data.size > 0) { // build associator
      val apriori: Apriori = new Apriori()
      apriori.setClassIndex(data.classIndex)
      apriori.buildAssociations(data)
      println(apriori)
      //println(String.valueOf(apriori))

    }
    else println("Not enough data provided")
  }


  //executePyConvertIntoNeo4J()
  executePyRunWEKA



}
//https://stackoverflow.com/questions/38657109/how-to-call-a-python-script-with-arguments-from-java-class