name := "graphARM"

version := "0.1"

scalaVersion := "2.12.6"

// https://mvnrepository.com/artifact/nz.ac.waikato.cms.weka/weka-stable
libraryDependencies += "nz.ac.waikato.cms.weka" % "weka-stable" % "3.8.0"

libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.4.4"

// https://mvnrepository.com/artifact/org.neo4j/neo4j
libraryDependencies += "org.neo4j" % "neo4j" % "3.4.6"


libraryDependencies += "com.jsuereth" %% "scala-arm" % "2.0"