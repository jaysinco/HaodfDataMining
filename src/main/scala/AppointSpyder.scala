/* [更新表名] doctor_basic
 * [运行时间] 10 sec
 * [依赖表名] doctor_basic(hospital_name,office_name,doctor_name)
 * [增加字段]
 * appoint_if      >>  医生 预约条件
 * appoint_time    >>  医生 预约时间
 * suc_appoint_num >>  医生 成功预约人数
 */

object AppointSpyder {
  import akka.actor.{ Actor, Props, ActorSystem }
  import akka.routing.FromConfig
  import org.jsoup.Jsoup
  import scala.collection.mutable
  type RowElem = Map[String, String]

  // Html Worker
  case class BetaAlphaWork(url: String)
  case class BetaAlphaWorkRtn(pgNum: Int, baseUrl: String)
  case class ReportDbUpdate(num: Int)
  case class BetaBetaWork(fullUrl: String)
  class Beta extends Actor {
    def receive = {
      case BetaAlphaWork(url) =>
        val pgNum = Toolkit.extractPageNum(Jsoup.parse(Toolkit.getHtml(url)), "div.p_bar a[rel=true].p_text")
        sender ! BetaAlphaWorkRtn(pgNum, url)
      case BetaBetaWork(pgUrl) =>
        val docRowElemList = parseHospAppUrl(pgUrl)
        context.actorFor("/user/alpha") ! ReportDbUpdate(docRowElemList.length)
        docRowElemList.foreach(context.actorFor("/user/dtbs") ! DtbsUpdate(_))
    }
    def parseHospAppUrl(pgUrl: String): List[RowElem] = {
      val hospAppTable = Jsoup.parse(Toolkit.getHtml(pgUrl)).select("table.listdoctor").first.select("tbody tr[align=center]")
      val hospAppIter = hospAppTable.iterator
      val docRowElems = new mutable.ListBuffer[RowElem]
      while (hospAppIter.hasNext) {
        val doctorElem = hospAppIter.next
        val hospOffice = doctorElem.select("td[width=12%]").first.text.split(" ")
        docRowElems += Map(
          "doctor_name" -> doctorElem.select("span.doctor_msg span a").first.text,
          "hospital_name" -> hospOffice(0).replace("中山一院", "中山大学附属第一医院").replace("301医院", "北京301医院").replace("北大医院", "北京大学第一医院").replace("上海中山医院", "复旦大学附属中山医院").replace("上海华山医院", "复旦大学附属华山医院"), // 解决医院名称不一致问题
          "office_name" -> hospOffice(1),
          "appoint_if" -> doctorElem.select("td[width=34%]").first.text,
          "appoint_time" -> doctorElem.select("td[width=24%]").first.text,
          "suc_appoint_num" -> Toolkit.extractNumFromString(doctorElem.select("td[width=10%]:contains(已成功)").first.text).toString)
      }
      docRowElems.toList
    }
  }

  // DataBase Processor
  case class DtbsUpdate(rowElem: RowElem)
  case object DtbsUpdateRtn
  class Dtbs(dbName: String, tbName: String) extends Toolkit.DtbsTemp(dbName, tbName) {
    val sqlSelect = "SELECT * FROM %s WHERE doctor_name = \"%s\" AND hospital_name = \"%s\" AND office_name = \"%s\"";
    val sqlUpdate = "UPDATE %s SET appoint_if = \"%s\", appoint_time = \"%s\", suc_appoint_num = \"%s\"" +
      "WHERE doctor_name = \"%s\" AND hospital_name = \"%s\" AND office_name = \"%s\""
    val sqlAddColumn = "ALTER TABLE %s ADD COLUMN %s TEXT DEFAULT \"N/A\"";
    List("appoint_if", "appoint_time", "suc_appoint_num").foreach(addKey =>
      sqlCon.prepareStatement(sqlAddColumn.format(tbName, addKey)).executeUpdate())

    def receive = {
      case DtbsUpdate(rowElem) =>
        val docTup = (rowElem("doctor_name"), rowElem("hospital_name"), rowElem("office_name"))
        if (!sqlCon.prepareStatement(sqlSelect.format(tbName, docTup._1, docTup._2, docTup._3)).executeQuery().next())
          Console.print("\n[info] Not Found [%s->%s->%s]\n[info] nOfRowElem(p) :      0".format(docTup._1, docTup._2, docTup._3))
        else sqlCon.prepareStatement(sqlUpdate.format(
          tbName, rowElem("appoint_if"), rowElem("appoint_time"), rowElem("suc_appoint_num"), docTup._1, docTup._2, docTup._3)).executeUpdate()
        entryInDtbs += 1
        Console.print("\b" * 6 + "% 6d".format(entryInDtbs)); Console.flush()
        context.actorFor("/user/alpha") ! DtbsUpdateRtn
    }
  }

  // Dispatcher
  case object Run
  class Alpha extends Actor {
    val startTime: Double = System.currentTimeMillis
    val betaPool = context.actorOf(Props[Beta].withRouter(FromConfig()).withDispatcher("spyder-dispatcher"), "betaPool")
    var expectA: Int = 0 // BetaAlphaWork 全部完成
    var countA: Int = 0
    var expectB: Int = 0 // DtbsUpdateRtn 全部完成
    var countB: Int = 0
    val hospAppUrlList = List(
      "http://jiahao.haodf.com/jiahao/search.htm?district=beijing&hospitalId=1",     // 1. 北京协和医院
      "http://jiahao.haodf.com/jiahao/search.htm?district=sichuan&hospitalId=488",   // 2. 华西医院
      "http://jiahao.haodf.com/jiahao/search.htm?district=beijing&hospitalId=335",   // 3. 北京301医院
      "http://jiahao.haodf.com/jiahao/search.htm?district=shanghai&hospitalId=424",  // 4. 上海瑞金医院
      "http://jiahao.haodf.com/jiahao/search.htm?district=shanxi&hospitalId=479",    // 5. 西京医院
      "http://jiahao.haodf.com/jiahao/search.htm?district=shanghai&hospitalId=418",  // 6. 华山医院
      "http://jiahao.haodf.com/jiahao/search.htm?district=shanghai&hospitalId=420",  // 7. 中山医院
      "http://jiahao.haodf.com/jiahao/search.htm?district=hubei&hospitalId=496",     // 8. 武汉同济医院
      "http://jiahao.haodf.com/jiahao/search.htm?district=guangdong&hospitalId=460", // 9. 中山大学附属第一医院
      "http://jiahao.haodf.com/jiahao/search.htm?district=beijing&hospitalId=23"     // 10. 北京大学第一医院
    )

    def receive = {
      case Run =>
        hospAppUrlList.foreach(betaPool ! BetaAlphaWork(_))
      case BetaAlphaWorkRtn(pgNum, baseUrl) =>
        expectA += pgNum
        (1 to pgNum).map("%s&page=%d".format(baseUrl, _)).foreach(betaPool ! BetaBetaWork(_))
      case ReportDbUpdate(num) =>
        countA += 1
        expectB += num
      case DtbsUpdateRtn =>
        countB += 1
        if (countA == expectA && countB == expectB) {
          Console.print("\n[info] AlphaTimer(s) :% 7.2f\n".format((System.currentTimeMillis - startTime) / 1000))
          context.system.shutdown()
        }
    }
  }

  //  RunMain
  def main(args: Array[String]) {
    val system = ActorSystem("AppointSpyderSystem")
    val alpha = system.actorOf(Props[Alpha], name = "alpha")
    val dtbs = system.actorOf(Props(new Dtbs("haodf.db", "doctor_basic")), name = "dtbs")
    alpha ! Run
  }
}
