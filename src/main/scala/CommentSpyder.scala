/* [创建表名] comment
 * [运行时间] 1500 sec
 * [依赖表名] doctor_basic(id,hospital_name,office_name,doctor_name,info_addr)
 * [字段内容]
 * id               >>  Auto
 * hospital_name    >>  医生 所属医院
 * office_name      >>  医生 所属科室
 * doctor_name      >>  医生 姓名
 * doctor_ref       >>  医生 在doctor_basic中的ID值
 * patient_name     >>  评论 病人名称
 * rate_cat         >>  评论 种类
 * rate_time        >>  评论 时间
 * disease          >>  评论 疾病名称
 * performance_rate >>  评论 疗效满意度
 * attitude_rate    >>  评论 态度满意度
 * comment_content  >>  评论 内容
 * value_num        >>  评论 推荐人数
 * reply_num        >>  评论 回应人数
 */
object CommentSpyder {
  import akka.actor.{ Actor, Props, ActorSystem }
  import akka.routing.FromConfig
  import org.jsoup.Jsoup
  import java.sql.ResultSet
  import java.util.regex.Pattern
  import org.jsoup.nodes.Document
  import org.jsoup.nodes.Element
  type RowElem = Map[String, String]

  // Html Worker
  case class BetaBetaWork(pgUrl: String, rowElem: RowElem)
  case class ReportDbWrite(num: Int)
  case class BetaAlphaWork(rowElem: RowElem)
  case class BetaAlphaWorkRtn(pgNum: Int, baseUrl: String, rowElem: RowElem)
  case class BetaGammaWork(rowElemList: List[RowElem])
  class Beta extends Actor {
    def receive = {
      case BetaAlphaWork(rowElem) =>
        val infoAddr = rowElem("info_addr")
        val (comBaseUrl, newRowElem) = (infoAddr.substring(0, infoAddr.length - 4) + "/alljingyan/", Map("doctor_ref" -> rowElem("id").toString) ++ rowElem - "info_addr" - "id")
        val artDoc = Jsoup.parse(Toolkit.getHtml(comBaseUrl + "1.htm"))
        val pgNum = Toolkit.extractPageNum(artDoc, "div.p_bar a[rel=true].p_text")
        sender ! BetaAlphaWorkRtn(pgNum, comBaseUrl, newRowElem)
        context.actorFor("/user/alpha/betaPool") ! BetaGammaWork(parseDocComPgDoc(artDoc, newRowElem))
      case BetaBetaWork(pgUrl, rowElem) =>
        reportAndWrite(parseDocComPgDoc(Jsoup.parse(Toolkit.getHtml(pgUrl)), rowElem))
      case BetaGammaWork(rowElemList) => // 这里不直接BetaAlpha写入数据库，是为了监控expectA
        reportAndWrite(rowElemList)
    }
    def reportAndWrite(docRowElemList: List[RowElem]) {
      context.actorFor("/user/alpha") ! ReportDbWrite(docRowElemList.length)
      docRowElemList.foreach(context.actorFor("/user/dtbs") ! DtbsWrite(_))
    }
    def parseDocComPgDoc(comDoc: Document, rowElem: RowElem): List[RowElem] = {
      val comElem = comDoc.getElementById("comment_content")
      val comElemList = if (comElem == null) List.empty[Element] else comElem.select("table.doctorjy").toArray.toList.asInstanceOf[List[Element]]
      for (commentElem <- comElemList) yield {
        val imgSrc = commentElem.select("tbody > tr:eq(1) img").first.attr("src")
        var typeStr: String = "N/A"
        if (imgSrc.matches(".+good_jy.gif")) typeStr = "看病经验"
        if (imgSrc.matches(".+good_gx.gif")) typeStr = "感谢信"
        val perforElem = commentElem.select("tbody > tr:eq(1) table tbody tr:eq(1) td:eq(1) span.orange").first
        val attElem = commentElem.select("tbody > tr:eq(1) table tbody tr:eq(2) td:eq(1) span.orange").first
        val recommNumStr = commentElem.select("tbody > tr:eq(2) span[id].orange").first.text
        val replyNumElem = commentElem.select("tbody > tr:eq(2) a.orange:contains(个回应)").first
        rowElem ++ Map(
          "rate_cat" -> typeStr,
          "patient_name" -> commentElem.select("tbody > tr:eq(1) table tbody tr:eq(0) td:eq(1)").first.text.replaceAll("患者：", ""),
          "rate_time" -> commentElem.select("tbody > tr:eq(1) table tbody tr:eq(0) td:eq(2)").first.text.replaceAll("时间：", ""),
          "disease" -> commentElem.select("tbody > tr:eq(1) table tbody tr:eq(1) td:eq(0)").first.text.replaceAll("疾病：", ""),
          "performance_rate" -> (if (perforElem == null) "N/A" else perforElem.text),
          "attitude_rate" -> (if (attElem == null) "N/A" else attElem.text),
          "comment_content" -> commentElem.select("tbody > tr:eq(2) td.spacejy").first.text,
          "value_num" -> (if (recommNumStr.equals("")) "N/A" else Toolkit.extractNumFromString(recommNumStr).toString),
          "reply_num" -> (if (replyNumElem == null) "N/A" else Toolkit.extractNumFromString(replyNumElem.text).toString))
      }
    }
  }

  // DataBase Processor
  case class DtbsWrite(rowElem: RowElem)
  case object DtbsWriteRtn
  case class DtbsQuery(sqlQuery: String)
  case class DtbsQueryRtn(queryRst: ResultSet)
  class Dtbs(dbName: String, tbName: String) extends Toolkit.DtbsTemp(dbName, tbName) {
    def receive = {
      case DtbsWrite(rowElem) =>
        if (!hasTable) hasTable = Toolkit.createDtbsTable(sqlCon, tbName, rowElem.keySet.toList)
        Toolkit.writeDtbsEntry(sqlCon, tbName, rowElem)
        entryInDtbs += 1
        Console.print("\b" * 9 + "% 9d".format(entryInDtbs)); Console.flush()
        context.actorFor("/user/alpha") ! DtbsWriteRtn
      case DtbsQuery(sqlQuery) =>
        sender ! DtbsQueryRtn(sqlCon.prepareStatement(sqlQuery).executeQuery())
    }
  }

  // Dispatcher
  case object Run
  class Alpha extends Actor {
    val startTime: Double = System.currentTimeMillis
    val betaPool = context.actorOf(Props[Beta].withRouter(FromConfig()).withDispatcher("spyder-dispatcher"), "betaPool")
    var expectA: Int = 0 // BetaBetaWork 全部完成
    var countA: Int = 0
    var expectB: Int = 0 // DtbsWriteRtn 全部完成
    var countB: Int = 0
    def receive = {
      case Run =>
        context.actorFor("/user/dtbs") ! DtbsQuery("SELECT * FROM doctor_basic")
      case DtbsQueryRtn(queryRst) =>
        while (queryRst.next) betaPool ! BetaAlphaWork(
          (for (key <- List("id", "hospital_name", "office_name", "doctor_name", "info_addr")) yield (key -> queryRst.getString(key))).toMap)
      case BetaAlphaWorkRtn(pgNum, commentBaseUrl, rowElem) =>
        expectA += pgNum
        if (pgNum > 1) (2 to pgNum).map("%s%d.htm".format(commentBaseUrl, _)).foreach(
          betaPool ! BetaBetaWork(_, rowElem))
      case ReportDbWrite(num) =>
        countA += 1
        expectB += num
      case DtbsWriteRtn =>
        countB += 1
        if (countA == expectA && countB == expectB) {
          Console.print("\n[info] AlphaTimer(s) :% 7.2f\n".format((System.currentTimeMillis - startTime) / 1000))
          context.system.shutdown()
        }
    }
  }

  //  RunMain
  def main(args: Array[String]) {
    val system = ActorSystem("CommentSpyderSystem")
    val alpha = system.actorOf(Props[Alpha], name = "alpha")
    val dtbs = system.actorOf(Props(new Dtbs("haodf.db", "comment")), name = "dtbs")
    alpha ! Run
  }
}