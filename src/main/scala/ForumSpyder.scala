/* [创建表名] forum
 * [运行时间] 820 sec (for 100 entries per doctor)
 * [依赖表名] doctor_basic(id,hospital_name,office_name,doctor_name,doctor_addr)
 * [字段内容]
 * id               >>  Auto
 * hospital_name    >>  医生 所属医院
 * office_name      >>  医生 所属科室
 * doctor_name      >>  医生 姓名
 * doctor_ref       >>  医生 在doctor_basic中的ID值
 * patient_name     >>  提问 病人名称
 * ask_name         >>  提问 标题
 * ask_property     >>  提问 属性【付费 隐私...】
 * relat_disease    >>  提问 相关疾病
 * total_word_num   >>  提问 总互动数
 * doctor_word_num  >>  提问 医生互动数
 * patient_word_num >>  提问 患者互动数
 * last_post        >>  提问 最后互动时间
 */
object ForumSpyder {
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
        val (forumBaseUrl, newRowElem) = (rowElem("doctor_addr") + "zixun/list.htm", Map("doctor_ref" -> rowElem("id").toString) ++ rowElem - "doctor_addr" - "id")
        val forumDoc = Jsoup.parse(Toolkit.getHtml(forumBaseUrl))
        val pgNum = Toolkit.extractPageNum(forumDoc, "div.page_turn a[rel=true].page_turn_a")
        sender ! BetaAlphaWorkRtn(pgNum, forumBaseUrl, newRowElem)
        context.actorFor("/user/alpha/betaPool") ! BetaGammaWork(parseDocArtPgDoc(forumDoc, newRowElem))
      case BetaBetaWork(pgUrl, rowElem) =>
        reportAndWrite(parseDocArtPgDoc(Jsoup.parse(Toolkit.getHtml(pgUrl)), rowElem))
      case BetaGammaWork(rowElemList) => // 这里不直接BetaAlpha写入数据库，是为了监控expectA
        reportAndWrite(rowElemList)
    }
    def reportAndWrite(docRowElemList: List[RowElem]) {
      context.actorFor("/user/alpha") ! ReportDbWrite(docRowElemList.length)
      docRowElemList.foreach(context.actorFor("/user/dtbs") ! DtbsWrite(_))
    }
    val WORD_NUM_PATT = Pattern.compile("(\\d+)\\s+\\((\\d+)/(\\d+)\\)")
    def parseDocArtPgDoc(forumDoc: Document, rowElem: RowElem): List[RowElem] = {
      val forumTableElem = forumDoc.select("div.zixun_list table tbody").first
      if (forumTableElem == null) List[RowElem]() // 医生暂无咨询
      else {
        val forumElementList = forumTableElem.select("tr:not(.zixun_list_title)").toArray.toList.asInstanceOf[List[Element]]
        for (forumElem <- forumElementList) yield {
          val propertyElemList = forumElem.select("td:eq(2) p img");
          val askProperty = if (propertyElemList.isEmpty) "N/A" else
            propertyElemList.toArray.toList.asInstanceOf[List[Element]].map(_.attr("title")) mkString ","
          val wordNumMatcher = WORD_NUM_PATT.matcher(forumElem.select("td:eq(4)").first.text)
          var tword, dword, pword = "N/A"
          if (wordNumMatcher.find) {
            tword = wordNumMatcher.group(1)
            dword = wordNumMatcher.group(2)
            pword = wordNumMatcher.group(3)
          } 
          rowElem ++ Map(
            "patient_name" -> forumElem.select("td:eq(1) p").first.text,
            "ask_name" -> forumElem.select("td:eq(2) p a").first.text,
            "relat_disease" -> forumElem.select("td:eq(3) a").first.text,
            "last_post" -> forumElem.select("td:eq(5) span.gray3").first.text,
            "ask_property" -> askProperty,
            "total_word_num" -> tword,
            "doctor_word_num" -> dword,
            "patient_word_num" -> pword)
        }
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
        context.actorFor("/user/dtbs") ! DtbsQuery("SELECT * FROM doctor_basic WHERE doctor_addr != \"N/A\"")
      case DtbsQueryRtn(queryRst) =>
        while (queryRst.next) betaPool ! BetaAlphaWork(
          (for (key <- List("id", "hospital_name", "office_name", "doctor_name", "doctor_addr")) yield (key -> queryRst.getString(key))).toMap)
      case BetaAlphaWorkRtn(pgNum, forumBaseUrl, rowElem) =>
        val actualNum = if (pgNum >= 4) 4 else pgNum
        expectA += actualNum
        if (actualNum > 1) (2 to actualNum).map("%s?type=&p=%d".format(forumBaseUrl, _)).foreach(
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
    val system = ActorSystem("ForumSpyderSystem")
    val alpha = system.actorOf(Props[Alpha], name = "alpha")
    val dtbs = system.actorOf(Props(new Dtbs("haodf.db", "forum")), name = "dtbs")
    alpha ! Run
  }
}