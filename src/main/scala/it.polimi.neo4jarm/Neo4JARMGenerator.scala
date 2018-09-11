package it.polimi.neo4jarm

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.util.Map
import java.util.Map.Entry

import org.neo4j.driver.v1._
import org.neo4j.graphdb.{GraphDatabaseService, Node, Result, Transaction}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import weka.core.Instances
import weka.core.converters.ConverterUtils.DataSource
import weka.associations.Apriori

import scala.sys.process._
import scala.io.Source
import resource._

import scala.collection.JavaConverters._


object Neo4JARMGenerator extends App {

  val ConvertForNeo4JImport = "convertneo4j"
  val BuildGraph = "buildgraph"
  val RunCypherQuery = "cypher"
  val ExtractMatrix = "matrix"
  val RunWEKA = "runweka"

  //to be checked after changing https://stackoverflow.com/questions/8865434/how-to-get-first-line-from-file-in-scala
  def firstLine(f: java.io.File): Option[String] = {
    managed(Source.fromFile(f)) acquireAndGet { src =>
      src.getLines.find(_ => true)
    }
  }

  def executePyConvertIntoNeo4J: Unit = {

    if (args.length < 10) {
      println(BuildGraph + " mode needs 9 parameters to be run:\n" +
        "(0) run mode\n" +
        "(1) location of files (e.g., import/original/)\n" +
        "(2) execution flag for first nodes (True/False)\n" +
        "(3) file name for first nodes (e.g., title.basics)\n" +
        "(4) execution flag for second nodes (True/False)\n" +
        "(5) file name for second nodes (e.g., name.basics)\n" +
        "(6) execution flag for relationships (True/False)\n" +
        "(7) file name for relationships (e.g., title.principals)\n" +
        "(8) progressive number range to indicate progress (e.g., if 10000, then programm prints 10000,20000,30000...)\n" +
        "(9) location of python script (e.g., it/polimi/pyConverter/csvToNeo4J.py)")
      System.exit(0)
    }

    val FILE_PATH = args(1)
    // val FILE_PATH = "/Users/abernasconi/Documents/neo4j-community-3.4.1/import/original/breve/"

    val flag_title = args(2)
    val TITLE_FILE_NAME = args(3)
    val TITLE_HEADER = firstLine(new File(FILE_PATH + TITLE_FILE_NAME + "_header.csv")).get

    //val TITLE_FILE_NAME = "title.basics"
    //val TITLE_FILE_NAME = "title.basics_breve"
    //val TITLE_HEADER = "tconst:ID,:LABEL,primaryTitle,originalTitle,isAdult:int,startYear,endYear,runtimeMinutes,:LABEL\n"

    val flag_name = args(4)
    val NAME_FILE_NAME = args(5)
    val NAME_HEADER = firstLine(new File(FILE_PATH + NAME_FILE_NAME + "_header.csv")).get

    //val NAME_FILE_NAME = "name.basics"
    //val NAME_FILE_NAME = "name.basics_breve"
    //val NAME_HEADER = "nconst:ID,primaryName,birthYear,deathYear,:LABEL\n"

    val flag_role = args(6)
    val ROLE_FILE_NAME = args(7)
    val ROLE_HEADER = firstLine(new File(FILE_PATH + ROLE_FILE_NAME + "_header.csv")).get
    //val ROLE_FILE_NAME = "title.principals"
    //val ROLE_FILE_NAME = "title.principals_breve"
    //val ROLE_HEADER = ":START_ID,ordering:int,:END_ID,:TYPE,job,characters:string[]\n"

    val num_cost = args(8)

    //val thisPath = getClass.getResource("").getPath.dropRight(1)
    //val pyScriptPath = thisPath.dropRight(thisPath.size - thisPath.lastIndexOf("/"))

    val pyScriptClass = args(9)

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

  def executeBashBuildGraph: Unit = {
    //ON SERVER
    //val result = "rm -rf data/databases/graph.db/; bin/neo4j-admin import --nodes import/original/title.basics_after.csv --nodes import/original/name.basics_after.csv --relationships import/original/title.principals_after.csv --ignore-missing-nodes=true" !!

    val remove_string = "rm -rf " + args(1)
    val exec_remove = remove_string.!
    val build_graph_string = args(2) + "neo4j-admin import --nodes " + args(3) + " --nodes " + args(4) + " --relationships " + args(5) + " --ignore-missing-nodes=true"
    val exec_import = build_graph_string.!
  }

  def executeCypherQuery: Unit = {

    if (args.length < 7) {
      println(RunCypherQuery + " mode needs 7 parameters to be run:\n" +
        "(0) run mode\n" +
        "(1) query for movies\n" +
        "(2) file name for movies (e.g., outMovies.csv)\n" +
        "(3) query for actors\n" +
        "(4) file name for actors (e.g., outActors.csv)\n" +
        "(5) query for relations\n" +
        "(6) file name for relations (e.g., outRel.csv)")
      System.exit(0)
    }

    val driver: Driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "yellow"))

    var list1: String = ""
    val query1 = args(1);
    try {
      val session: Session = driver.session()
      try {
        val result: StatementResult = session.run(query1);
        while (result.hasNext()) {
          list1 = list1 + "\n" + result.next()
        }
      }
    }
    val file1 = new File(args(2))
    val bw1 = new BufferedWriter(new FileWriter(file1))
    bw1.write(list1)
    bw1.close()

    var list2:String = ""
    val query2 = args(3);
    try {
      val session: Session = driver.session()
      try {
        val result: StatementResult = session.run(query2);
        while (result.hasNext()) {
          list2 = list2 + "\n" + result.next().toString
        }
      }
    }
    val file2 = new File(args(4))
    val bw2 = new BufferedWriter(new FileWriter(file2))
    bw2.write(list2)
    bw2.close()

    var list3:String = ""
    val query3 = args(5);
    try {
      val session: Session = driver.session()
      try {
        val result: StatementResult = session.run(query3);
        while (result.hasNext()) {
          list3 = list3 + "\n" + result.next().toString
        }
      }
    }
    val file3 = new File(args(6))
    val bw3 = new BufferedWriter(new FileWriter(file3))
    bw3.write(list3)
    bw3.close()



    //https://dzone.com/articles/getting-started-neo4j-with-scala-annbspintroductio

    /*  val driver = GraphDatabase.driver("bolt://localhost/7474", AuthTokens.basic("neo4j", "neo4j"))
      val session = driver.session
      val script = args(1)
      val result = session.run(script)
      println(result)
      new PrintWriter("outActors.csv") {
        write(result.toString); close
      }*/

    // val start_shell_string = "bin/neo4j-shell -path ~/neo4j-community-3.4.1/data/databases/graph.db"
    /*   val start_shell_string = "/Users/abernasconi/Documents/neo4j-community-3.4.1/bin/neo4j-shell -path /Users/abernasconi/Documents/neo4j-community-3.4.1/data/databases/graph.db/"
       val result0 = start_shell_string.!

       val import_movies_string = "import-cypher -o outMovies.csv MATCH (action_movie:Action) WHERE action_movie.primaryTitle =~ \"C.*\" WITH action_movie ORDER BY action_movie.primaryTitle ASC LIMIT 3 RETURN action_movie.tconst;"
       val result1 = import_movies_string.!
       //println(result)
       val import_actors_string = "import-cypher -o outActors.csv MATCH (action_movie:Action) WHERE action_movie.primaryTitle =~ \"C.*\" WITH action_movie ORDER BY action_movie.primaryTitle ASC LIMIT 3 MATCH (action_movie:Action)-[relation:actor]-(person:actor) RETURN person.nconst UNION ALL MATCH (action_movie:Action) WHERE action_movie.primaryTitle =~ \"C.*\" WITH action_movie ORDER BY action_movie.primaryTitle ASC LIMIT 3 MATCH (action_movie:Action)-[relation:actress]-(person:actress) RETURN person.nconst;"
       val result2 = import_actors_string.!
       //println(result2)
       val import_rel_string = "import-cypher -o outRel.csv MATCH (action_movie:Action) WHERE action_movie.primaryTitle =~ \"C.*\" WITH action_movie ORDER BY action_movie.primaryTitle ASC LIMIT 3 MATCH (action_movie:Action)-[relation:actor]-(person:actor) RETURN startNode(relation).tconst,endNode(relation).nconst UNION ALL MATCH (action_movie:Action) WHERE action_movie.primaryTitle =~ \"C.*\" WITH action_movie ORDER BY action_movie.primaryTitle ASC LIMIT 3 MATCH (action_movie:Action)-[relation:actress]-(person:actress) RETURN startNode(relation).tconst,endNode(relation).nconst;"
       val result3 = import_rel_string.!

       val stop_shell_string = "quit"
       val result4 = stop_shell_string.!
 */
  }

  def executePyQueryResultIntoMatrix: Unit = {
    if (args.length < 7) {
      println(ExtractMatrix + " mode needs 7 parameters to be run:\n" +
        "(0) run mode\n" +
        "(1) location of files (e.g., /home/bernasconi/neo4j-community-3.4.1/)\n" +
        "(2) file name for first nodes (e.g., att.csv)\n" +
        "(3) file name for second nodes (e.g., mov.csv)\n" +
        "(4) file name for relationships (e.g., rel.csv)\n" +
        "(5) file name to output arff matrix (e.g., matrix.arff)\n" +
        "(6) location of python script (e.g., it/polimi/pyConverter/cypherResultToArffMatrix.py)")
      System.exit(0)
    }

    val FILE_PATH = args(1)
    // FILE_PATH = "/Users/abernasconi/Downloads/"

    val NAME_FILE_NAME = args(2)
    //IN_ACT = "att.csv"

    val TITLE_FILE_NAME = args(3)
    //IN_MOV = "mov.csv"

    val ROLE_FILE_NAME = args(4)
    //IN_REL = "rel.csv"

    val MATRIX_FILE_NAME = args(5)
    //IN_REL = "rel.csv"

    val pyScriptClass = args(6)

    val result = "python " +
      pyScriptClass + " " +
      FILE_PATH + " " +
      NAME_FILE_NAME + " " +
      TITLE_FILE_NAME + " " +
      ROLE_FILE_NAME + " " +
      MATRIX_FILE_NAME !

  }

  def runWEKA: Unit = {

    if (args.length < 2) {
      println(ExtractMatrix + " mode needs 2 parameters to be run:\n" +
        "(0) run mode\n" +
        "(1) location of matrix file (e.g., /home/bernasconi/neo4j-community-3.4.1/matrix.arff)")
      System.exit(0)
    }

    val source: DataSource = new DataSource(args(1))
    //val source: DataSource = new DataSource("/Users/abernasconi/Downloads/matrix.arff")
    val data: Instances = source.getDataSet()
    if (data.classIndex() == -1)
      data.setClassIndex(data.numAttributes() - 1)

    if (data.size > 0) { // build associator
      val apriori: Apriori = new Apriori()
      apriori.setClassIndex(data.classIndex)
      apriori.buildAssociations(data)
      println(String.valueOf(apriori))

      //https://www.programcreek.com/java-api-examples/?api=weka.associations.Apriori

    }
    else println("Not enough data provided")
  }


  if (args.length < 1) {
    println("Error! Please re-run the app specifying one of the following modes:\n" +
      BuildGraph + "\n" +
      ExtractMatrix + "\n" +
      RunWEKA)
    System.exit(0)
  }

  args(0) match {
    case ConvertForNeo4JImport => executePyConvertIntoNeo4J
    case BuildGraph => executeBashBuildGraph
    case RunCypherQuery => executeCypherQuery
    case ExtractMatrix => executePyQueryResultIntoMatrix
    case RunWEKA => runWEKA
    case _ => "Invalid run mode" // the default, catch-all
  }


}

//https://stackoverflow.com/questions/38657109/how-to-call-a-python-script-with-arguments-from-java-class