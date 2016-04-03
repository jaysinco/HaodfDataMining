/* [创建表名] office
 * [运行时间] 60 sec for 10 hospital
 * [依赖表名] 无
 * [字段内容]
 * id                 >>  Auto
 * hospital_name      >>  科室 所属医院
 * office_name        >>  科室 名称
 * office_addr        >>  科室 网址
 * doctor_num         >>  科室 开通加号服务大夫人数
 * patient_num        >>  科室 已成功加号患者人数
 * contact_doctor_num >>  科室 可直接通话大夫人数
 * office_intro       >>  科室 简介
 */

object OfficeSpyder {
  import akka.actor.{ Actor, Props, ActorSystem }
  import akka.routing.FromConfig
  import org.jsoup.Jsoup
  import org.jsoup.nodes.Element
  import scala.collection.mutable
  type RowElem = Map[String, String]

  // Html Worker
  case class BetaAlphaWork(rowElem: RowElem)
  case class BetaBetaWork(url: String)
  case class BetaBetaWorkRtn(rowElemList: List[RowElem])
  class Beta extends Actor {
    def receive = {
      case BetaAlphaWork(rowElem) => context.actorFor("/user/dtbs") ! DtbsWrite(parseOffcUrl(rowElem))
      case BetaBetaWork(url) => sender ! BetaBetaWorkRtn(parseHospUrl(url))
    }
    def parseOffcUrl(rowElem: RowElem): RowElem = {
      val officeUrl = rowElem("office_addr")
      val officeInfoTable = Jsoup.parse(Toolkit.getHtml(officeUrl)).getElementById("about")
      var officeIntro = Jsoup.parse(Toolkit.getHtml(officeUrl.substring(0, officeUrl.length - 4) + "/jieshao.htm")).getElementById("about_det").text
      val offcRepMap = Map("\"" -> "", "我来添加/修改此科室介绍" -> "", "\\s" -> "", "\u3000" -> "", "暂无科室介绍" -> "N/A")
      for (repKey <- offcRepMap.keySet) officeIntro = officeIntro.replace(repKey, offcRepMap(repKey))
      rowElem ++ Map(
        "doctor_num" -> dftVal(officeInfoTable.select("tbody tr td div[class=\"clearfix\"] span[class=\"fl\"] span[class=\"orange\"]").first),
        "patient_num" -> dftVal(officeInfoTable.select("tbody tr td p span span[class=\"orange\"]").first),
        "contact_doctor_num" -> dftVal(officeInfoTable.select("tbody tr td[class=\"pb10\"] span span[class=\"orange\"]").first),
        "office_intro" -> officeIntro)
    }
    def parseHospUrl(url: String): List[RowElem] = {
      val hospDoc = Jsoup.parse(Toolkit.getHtml(url))
      val hospName = hospDoc.getElementById("ltb").text
      val offcLinks = (hospDoc.getElementById("hosbra")).select("tbody tr td[width=\"50%\"] a[class=\"blue\"]")
      val offcRowElems = new mutable.ListBuffer[RowElem]
      val offcLinksIter = offcLinks.iterator
      while (offcLinksIter.hasNext) {
        val offcLink = offcLinksIter.next
        offcRowElems += Map("hospital_name" -> hospName, "office_name" -> offcLink.text, "office_addr" -> offcLink.attr("href"))
      }
      offcRowElems.toList
    }
    def dftVal(elem: Element) = if (elem == null) "N/A" else elem.text
  }

  // DataBase Processor
  case class DtbsWrite(rowElem: RowElem)
  case object DtbsWriteRtn
  class Dtbs(dbName: String, tbName: String) extends Toolkit.DtbsTemp(dbName, tbName) {
    def receive = {
      case DtbsWrite(rowElem) =>
        if (!hasTable) hasTable = Toolkit.createDtbsTable(sqlCon, tbName, rowElem.keySet.toList)
        Toolkit.writeDtbsEntry(sqlCon, tbName, rowElem)
        entryInDtbs += 1
        Console.print("\b" * 6 + "% 6d".format(entryInDtbs)); Console.flush()
        context.actorFor("/user/alpha") ! DtbsWriteRtn
    }
  }

  // Dispatcher
  case object Run
  class Alpha extends Actor {
    val startTime: Double = System.currentTimeMillis
    val hospUrls = List(
      "http://www.haodf.com/hospital/DE4rIxMvCogR-EoxrURJ3U3.htm",   // 1. 北京协和医院
      "http://www.haodf.com/hospital/DE4roiYGYZwXJq43vTNHYjTjw.htm", // 2. 华西医院
      "http://www.haodf.com/hospital/DE4roiYGYZw0hEIKBtDEBxt8k.htm", // 3. 北京301医院
      "http://www.haodf.com/hospital/DE4roiYGYZwXhEaCFVHDuJVht.htm", // 4. 上海瑞金医院
      "http://www.haodf.com/hospital/DE4roiYGYZwX-q9quPKsNiPfO.htm", // 5. 西京医院
      "http://www.haodf.com/hospital/DE4roiYGYZwXGaOyZJ9SvRJb8.htm", // 6. 华山医院
      "http://www.haodf.com/hospital/DE4roiYGYZwXhYmS30yF9V0wc.htm", // 7. 中山医院
      "http://www.haodf.com/hospital/DE4roiYGYZwXyb43vTNHYjTjw.htm", // 8. 武汉同济医院
      "http://www.haodf.com/hospital/DE4roiYGYZwXiEUz-mzZSmmxP.htm", // 9. 中山大学附属第一医院
      "http://www.haodf.com/hospital/DE4raCNSz6ONSaQTyscBtyq4.htm"   // 10. 北京大学第一医院
      )
    val betaPool = context.actorOf(Props[Beta].withRouter(FromConfig()).withDispatcher("spyder-dispatcher"), "betaPool")
    var expect: Int = 0
    var count: Int = 0
    def receive = {
      case Run =>
        hospUrls.foreach(betaPool ! BetaBetaWork(_))
      case BetaBetaWorkRtn(rowElemList) =>
        expect += rowElemList.length
        rowElemList.foreach(betaPool ! BetaAlphaWork(_))
      case DtbsWriteRtn =>
        count += 1
        if (count == expect) {
          Console.print("\n[info] AlphaTimer(s) :% 7.2f\n".format((System.currentTimeMillis - startTime) / 1000))
          context.system.shutdown()
        }
    }
  }

  // RunMain
  def main(args: Array[String]) {
    val system = ActorSystem("OfficeSpyderSystem")
    val alpha = system.actorOf(Props[Alpha], name = "alpha")
    val dtbs = system.actorOf(Props(new Dtbs("haodf.db", "office")), name = "dtbs")
    alpha ! Run
  }
}