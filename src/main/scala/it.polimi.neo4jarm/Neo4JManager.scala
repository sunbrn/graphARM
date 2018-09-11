package it.polimi.neo4jarm

import org.neo4j.driver.v1._


object Neo4JManager {
  @throws[Exception]
  def main(args: String*): Unit = {
    try {
      val greeter = new Neo4JManager("bolt://localhost:7687", "neo4j", "password")
      try
        greeter.printGreeting("hello, world")
      finally if (greeter != null) greeter.close()
    }
  }
}

class Neo4JManager(val uri: String, val user: String, val password: String) extends AutoCloseable {
  val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))

  @throws[Exception]
  override def close(): Unit = {
    driver.close
  }

  def printGreeting(message: String): Unit = {
    try {
      val session: Session = driver.session
      try {
        val greeting = session.writeTransaction(new TransactionWork[String]{
          def execute(tx: Transaction): String = {
            val result = tx.run("MATCH (n) WHERE id(n)=2 RETURN n;")
           // val result = tx.run("CREATE (a:Greeting) " + "SET a.message = $message " + "RETURN a.message + ', from node ' + id(a)", parameters("message", message))
            result.single.get(0).asString
          }
        })
        System.out.println(greeting)
      } finally if (session != null) session.close()
    }
  }
}

