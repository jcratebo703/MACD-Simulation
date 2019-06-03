package tests

import java.sql.{Connection, DriverManager, PreparedStatement}

object DBTest extends App {
  // connect to the database named "mysql" on port 3306 of localhost
  val url = "jdbc:mysql://localhost:3306/mysql"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "jcratebo703"
  var connection: Connection = null
  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
//    val rs = statement.executeQuery("SELECT Name, TranFrequency FROM scalaTest.cop")
//    while (rs.next) {
//      val name = rs.getString("Name")
//      val freq = rs.getInt("TranFrequency")
//      println("name = %s, freq = %d".format(name,freq))
//    }

    val insertSQL = "INSERT INTO scalaTest.cop (Name, TranFrequency) VALUES(?, ?)"

    val prep: PreparedStatement = connection.prepareStatement(insertSQL)

    prep.setString(1, "DBTest1")
    prep.setInt(2, 4554)
    prep.execute()

    prep.close()

  } catch {
    case e: Exception => e.printStackTrace
  }
  connection.close

}
