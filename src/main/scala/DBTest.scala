package tests

import java.sql.{Connection,DriverManager}

object DBTest extends App {
  // connect to the database named "mysql" on port 3306 of localhost
  val url = "jdbc:mysql://localhost:3306/mysql"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "jcratebo703"
  var connection: Connection = _
  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
    val rs = statement.executeQuery("SELECT Name, TranFrequency FROM scalaTest.cop")
    while (rs.next) {
      val name = rs.getString("Name")
      val freq = rs.getInt("TranFrequency")
      println("name = %s, freq = %d".format(name,freq))
    }
  } catch {
    case e: Exception => e.printStackTrace
  }
  connection.close
}
