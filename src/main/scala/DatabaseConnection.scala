import java.sql.{Connection, DriverManager, PreparedStatement}

class DatabaseConnection(val opIndex: String, val ERate: Double, val CRate: Double, val transFreq: Int) {
  val url = "jdbc:mysql://localhost:3306/mysql"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "jcratebo703"
  var connection: Connection = _

  def writeDB(databaseName: String): Unit ={
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      //val statement = connection.createStatement
      //    val rs = statement.executeQuery("SELECT Name, TranFrequency FROM scalaTest.cop")
      //    while (rs.next) {
      //      val name = rs.getString("Name")
      //      val freq = rs.getInt("TranFrequency")
      //      println("name = %s, freq = %d".format(name,freq))
      //    }

      val insertSQL = "INSERT INTO scalaTest."+ databaseName + " (parameters, ERate, CRate, Frequency)" +
        " VALUES(?, ?, ?, ?)"

      val prep: PreparedStatement = connection.prepareStatement(insertSQL)

      prep.setString(1, opIndex)
      prep.setDouble(2, ERate)
      prep.setDouble(3, CRate)
      prep.setInt(4, transFreq)
      prep.execute()

      prep.close()

    } catch {
      case e: Exception => e.printStackTrace
    }
    connection.close
  }

}
