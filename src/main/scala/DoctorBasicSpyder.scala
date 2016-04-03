/* [创建表名] doctor_basic
 * [运行时间] 60 sec 
 * [依赖表名] office(hospital_name,office_name,office_addr)
 * [字段内容]
 * id              >>  Auto
 * hospital_name   >>  医生 所属医院
 * office_name     >>  医生 所属科室
 * doctor_name     >>  医生 姓名
 * info_addr       >>  医生 好大夫主页
 * title_name      >>  医生 职称 学历
 * recommend_index >>  医生 患者推荐热度
 * doctor_addr     >>  医生 个人主页
 * can_change      >>  医生 能否转诊
 * can_call        >>  医生 能否通电话
 * can_ask         >>  医生 能否咨询
 */

object DoctorBasicSpyder {
  import akka.actor.{ Actor, Props, ActorSystem }
  import akka.routing.FromConfig
  import org.jsoup.Jsoup
  import org.jsoup.nodes.Document
  import org.jsoup.nodes.Element
  import scala.collection.mutable
  import java.sql.ResultSet
  import java.util.regex.Pattern
  type RowElem = Map[String, String]

  // Html Worker
  case class BetaAlphaWork(pgUrl: String, rowElem: RowElem)
  case class ReportDbWrite(num: Int)
  case class BetaBetaWork(rowElem: RowElem)
  case class BetaBetaWorkRtn(pgNum: Int, baseUrl: String, rowElem: RowElem)
  case class BetaGammaWork(rowElemList: List[RowElem])
  class Beta extends Actor {
    val CAN_CHANGE_PAT = Pattern.compile("可转.*诊")
    val CAN_CALL_PAT = Pattern.compile("可通电话")
    val CAN_ASK_PAT = Pattern.compile("可咨询")
    def receive = {
      case BetaBetaWork(rowElem) =>
        val (offcUrl, newRowElem) = (rowElem("office_addr"), rowElem - "office_addr")
        val offcDoc = Jsoup.parse(Toolkit.getHtml(offcUrl))
        val pgNum = Toolkit.extractPageNum(offcDoc, "div.p_bar a[rel=true].p_text")
        sender ! BetaBetaWorkRtn(pgNum, offcUrl, newRowElem)
        context.actorFor("/user/alpha/betaPool") ! BetaGammaWork(parseOffcPgDoc(offcDoc, newRowElem))
      case BetaAlphaWork(pgUrl, rowElem) =>
        reportAndWrite(parseOffcPgDoc(Jsoup.parse(Toolkit.getHtml(pgUrl)), rowElem))
      case BetaGammaWork(rowElemList) =>
        reportAndWrite(rowElemList)
    }

    def reportAndWrite(docRowElemList: List[RowElem]) {
      context.actorFor("/user/alpha") ! ReportDbWrite(docRowElemList.length)
      docRowElemList.foreach(context.actorFor("/user/dtbs") ! DtbsWrite(_))
    }

    def parseOffcPgDoc(offcPgDoc: Document, rowElem: RowElem): List[RowElem] = {
      val docElemList = offcPgDoc.getElementById("doc_list_index").select("tbody tr")
      val docElemListIter = docElemList.iterator
      val docRowElems = new mutable.ListBuffer[RowElem]
      while (docElemListIter.hasNext) {
        val docElem = docElemListIter.next
        val firstColumn = docElem.select("td.tdnew_a li").first
        val docNameInfo = firstColumn.select("a.name").first
        val recomIndexElem = docElem.select("span.patient_recommend i.bigred").first
        val lastColumn = docElem.select("td.tdnew_d").first
        val perWebsite = lastColumn.select("li a[title~=个人网站]").first
        val lastColumnStr = lastColumn.text
        docRowElems += (rowElem ++ Map(
          "doctor_name" -> docNameInfo.text,
          "info_addr" -> docNameInfo.attr("href"),
          "title_name" -> firstColumn.select("p").text,
          "recommend_index" -> (if (recomIndexElem == null) "N/A" else recomIndexElem.text),
          "doctor_addr" -> (if (perWebsite == null) "N/A" else perWebsite.attr("href")),
          "can_change" -> (if (CAN_CHANGE_PAT.matcher(lastColumnStr).find) "1" else "0"),
          "can_call" -> (if (CAN_CALL_PAT.matcher(lastColumnStr).find) "1" else "0"),
          "can_ask" -> (if (CAN_ASK_PAT.matcher(lastColumnStr).find) "1" else "0")))
      }
      docRowElems.toList
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
        Console.print("\b" * 6 + "% 6d".format(entryInDtbs)); Console.flush()
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
    var expectA: Int = 0 // DtbsWriteRtn 全部完成
    var countA: Int = 0
    var expectB: Int = 0 // BetaAlphaWork 全部完成
    var countB: Int = 0
    def receive = {
      case Run =>
        context.actorFor("/user/dtbs") ! DtbsQuery("SELECT hospital_name,office_name,office_addr FROM office")
      case DtbsQueryRtn(queryRst) =>
        while (queryRst.next) betaPool ! BetaBetaWork(
          (for (key <- List("hospital_name", "office_name", "office_addr")) yield (key -> queryRst.getString(key))).toMap)
      case BetaBetaWorkRtn(pgNum, baseUrl, rowElem) =>
        expectB += pgNum
        if (pgNum > 1) (2 to pgNum).map("%s/menzhen_%d.htm".format(baseUrl.split("\\.htm")(0), _)).foreach(
          betaPool ! BetaAlphaWork(_, rowElem))
      case ReportDbWrite(num) =>
        countB += 1
        expectA += num
      case DtbsWriteRtn =>
        countA += 1
        if (countA == expectA && countB == expectB) {
          Console.print("\n[info] AlphaTimer(s) :% 7.2f\n".format((System.currentTimeMillis - startTime) / 1000))
          context.system.shutdown()
        }
    }
  }

  //  RunMain
  def main(args: Array[String]) {
    val system = ActorSystem("DoctorBasicSpyderSystem")
    val alpha = system.actorOf(Props[Alpha], name = "alpha")
    val dtbs = system.actorOf(Props(new Dtbs("haodf.db", "doctor_basic")), name = "dtbs")
    alpha ! Run
  }
}