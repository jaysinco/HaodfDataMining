// 功能函数
object Toolkit {
  import java.net.URL
  import java.io.{ InputStreamReader, BufferedReader }
  import java.sql.Connection
  import scala.collection.mutable
  import org.jsoup.nodes.Document
  import java.util.regex.Pattern
  import java.sql.DriverManager
  import akka.actor.Actor

  abstract class DtbsTemp(dbName: String, tbName: String) extends Actor {
    Console.print("[info] nOfRowElem(p) :         0"); Console.flush()
    Class.forName("org.sqlite.JDBC")
    val sqlCon = DriverManager.getConnection("jdbc:sqlite:%s".format(dbName))
    sqlCon.setAutoCommit(false)
    var hasTable = getDtbsSchema(sqlCon).contains(tbName)
    var entryInDtbs = 0
    override def postStop: Unit =
      if (sqlCon != null) { sqlCon.commit(); sqlCon.close() }
  }

  def writeDtbsEntry(sqlCon: Connection, tbName: String, entryMap: Map[String, String]) {
    val sqlInsert = "INSERT INTO %s(%s) VALUES(%s)"
    val keyList = entryMap.keySet.toList
    val nameTupleStr = keyList mkString ","
    val valueTupleStr = keyList.map(entryMap(_).replaceAll("\"", "'")).mkString("\"", "\",\"", "\"")
    sqlCon.prepareStatement(sqlInsert.format(tbName, nameTupleStr, valueTupleStr)).executeUpdate()
  }

  def createDtbsTable(sqlCon: Connection, tbName: String, keyList: List[String]): Boolean = {
    val sqlCtbl = "CREATE TABLE %s(id INTEGER PRIMARY KEY AUTOINCREMENT,%s)";
    val keyTypeStr = keyList.map(_ + " TEXT") mkString ","
    sqlCon.prepareStatement(sqlCtbl.format(tbName, keyTypeStr)).executeUpdate()
    true
  }

  def getDtbsSchema(sqlCon: Connection): mutable.Set[String] = {
    val sqlSchema = mutable.Set.empty[String]
    val rst = sqlCon.prepareStatement("SELECT name FROM sqlite_master WHERE type='table'").executeQuery()
    while (rst.next) sqlSchema += rst.getString("name")
    sqlSchema
  }

  def getHtml(htmlUrl: String, encoding: String = "GBK"): String = {
    val rtnHtml = new StringBuilder
    val conn = new URL(htmlUrl).openConnection();
    conn.setRequestProperty("user-agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:44.0) Gecko/20100101 Firefox/44.0")
    val breader = new BufferedReader(new InputStreamReader(conn.getInputStream, encoding))
    var temp = breader.readLine
    while (temp != null) {
      rtnHtml.append(temp).append("\n")
      temp = breader.readLine
    }
    rtnHtml.toString
  }

  def extractPageNum(htmlDoc: Document, selectStr: String): Int = {
    val pageNumEle = htmlDoc.select(selectStr).first
    if (pageNumEle == null) 1 else extractNumFromString(pageNumEle.text)
  }

  val DIGITAL_PAT = Pattern.compile("(\\d+)")
  def extractNumFromString(str: String): Int = {
    val matcher = DIGITAL_PAT.matcher(str)
    if (matcher.find) matcher.group(1).toInt else throw new Exception("extractNumFromString failed")
  }

  val RE_UNICODE = Pattern.compile("\\\\u([0-9a-zA-Z]{4})")
  def decodeUnicodeEscape(s: String): String = {
    val m = RE_UNICODE.matcher(s)
    val buffer = new StringBuffer(s.length())
    while (m.find) m.appendReplacement(buffer, Integer.parseInt(m.group(1), 16).toChar.toString)
    m.appendTail(buffer)
    buffer.toString
  }

  def main(args: Array[String]) {
    println(getHtml("http://www.haodf.com"))
  }
}

