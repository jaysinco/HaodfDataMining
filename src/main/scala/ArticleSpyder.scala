/* [创建表名] article
 * [运行时间] 227 sec
 * [依赖表名] doctor_basic(id,hospital_name,office_name,doctor_name,doctor_addr)
 * [字段内容]
 * id              >>  Auto
 * hospital_name   >>  医生 所属医院
 * office_name     >>  医生 所属科室
 * doctor_name     >>  医生 姓名
 * doctor_ref      >>  医生 在doctor_basic中的ID值
 * article_cap     >>  文章 所属类别
 * article_title   >>  文章 标题
 * read_num        >>  文章 阅读人数
 * publish_time    >>  文章 发表时间
 */
object ArticleSpyder {
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
        val (articleBaseUrl, newRowElem) = (rowElem("doctor_addr") + "lanmu", Map("doctor_ref" -> rowElem("id").toString) ++ rowElem - "doctor_addr" - "id")
        val artDoc = Jsoup.parse(Toolkit.getHtml(articleBaseUrl))
        val pgNum = Toolkit.extractPageNum(artDoc, "div.page_turn a[rel=true].page_turn_a")
        sender ! BetaAlphaWorkRtn(pgNum, articleBaseUrl, newRowElem)
        context.actorFor("/user/alpha/betaPool") ! BetaGammaWork(parseDocArtPgDoc(artDoc, newRowElem))
      case BetaBetaWork(pgUrl, rowElem) =>
        reportAndWrite(parseDocArtPgDoc(Jsoup.parse(Toolkit.getHtml(pgUrl)), rowElem))
      case BetaGammaWork(rowElemList) => // 这里不直接BetaAlpha写入数据库，是为了监控expectA
        reportAndWrite(rowElemList)
    }
    def reportAndWrite(docRowElemList: List[RowElem]) {
      context.actorFor("/user/alpha") ! ReportDbWrite(docRowElemList.length)
      docRowElemList.foreach(context.actorFor("/user/dtbs") ! DtbsWrite(_))
    }
    val PUBLISH_TIME_PATT = Pattern.compile("发表于([\\d-].+)")
    def parseDocArtPgDoc(artDoc: Document, rowElem: RowElem): List[RowElem] = {
      val artElement = artDoc.select("ul.article_ul").first
      val artElementList = if (artElement == null) List.empty[Element] else artElement.select("li").toArray.toList.asInstanceOf[List[Element]]
      for (artElem <- artElementList) yield {
        val ptMatcher = PUBLISH_TIME_PATT.matcher(artElem.select("p[class=\"fr read_article\"]").first.text)
        val capName = artElem.select("a[class=\"pr5 art_cate\"]").first
        val titleName = artElem.select("a[class=art_t]").first
        val readNum = artElem.select("p[class=\"fr read_article\"] span").first
        rowElem ++ Map(
          "article_cap" -> (if (capName == null) "N/A" else capName.text),
          "article_title" -> (if (titleName == null) "N/A" else titleName.text),
          "read_num" -> (if (readNum == null) "N/A" else readNum.text),
          "publish_time" -> (if (ptMatcher.find) ptMatcher.group(1) else "N/A"))
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
      case BetaAlphaWorkRtn(pgNum, articleBaseUrl, rowElem) =>
        expectA += pgNum
        if (pgNum > 1) (2 to pgNum).map("%s_%d".format(articleBaseUrl, _)).foreach(
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
    val system = ActorSystem("ArticleSpyderSystem")
    val alpha = system.actorOf(Props[Alpha], name = "alpha")
    val dtbs = system.actorOf(Props(new Dtbs("haodf.db", "article")), name = "dtbs")
    alpha ! Run
  }
}