/* [更新表名] doctor_basic
 * [运行时间] 350 sec
 * [依赖表名] doctor_basic(id,can_call,can_ask,doctor_addr)
 * [增加字段]
 * phone_cost_info   >>  医生 电话咨询收费标准
 * patient_ask_num   >>  医生 患者网上咨询服务总人数
 */

object ExtraSpyder {
  import akka.actor.{ Actor, Props, ActorSystem }
  import akka.routing.FromConfig
  import org.jsoup.Jsoup
  import java.sql.ResultSet
  type RowElem = Map[String, String]

  // Html Worker
  case class BetaWork(rowElem: RowElem)
  class Beta extends Actor {
    def receive = {
      case BetaWork(rowElem) =>
        context.actorFor("/user/dtbs") ! DtbsUpdate(parseDocAddr(rowElem))
    }
    def parseDocAddr(rowElem: RowElem): RowElem =
      if (rowElem("can_call") == "0" && rowElem("can_ask") == "0") Map.empty[String, String]
      else Map(
        "phone_cost_info" -> (if (rowElem("can_call") == "0") "N/A" else getPhoneCost(rowElem("doctor_addr"))),
        "patient_ask_num" -> (if (rowElem("can_ask") == "0") "N/A" else getAskNum(rowElem("doctor_addr"))),
        "id" -> rowElem("id"))
    def getPhoneCost(doctAddr: String) = {
      val phoneCostElem = Jsoup.parse(Toolkit.getHtml(doctAddr + "payment/newintro")).select("ul[class=\"clearfix i_top\"]").first
      if (phoneCostElem == null) "N/A" else phoneCostElem.text.replaceAll("／", "/")
    }
    def getAskNum(doctAddr: String) = {
      val askNumElem = Jsoup.parse(Toolkit.getHtml(doctAddr + "zixun/list.htm")).select("span[class=\"pl5 fs\"] span[class=\"f14 orange1\"]").first
      if (askNumElem == null) "N/A" else askNumElem.text
    }
  }

  // DataBase Processor
  case class DtbsUpdate(rowElem: RowElem)
  case object DtbsUpdateRtn
  case class DtbsQuery(sqlQuery: String)
  case class DtbsQueryRtn(queryRst: ResultSet)
  class Dtbs(dbName: String, tbName: String) extends Toolkit.DtbsTemp(dbName, tbName) {
    val sqlUpdate = "UPDATE %s SET phone_cost_info = \"%s\",patient_ask_num = \"%s\" WHERE id = %s"
    val sqlAddColumn = "ALTER TABLE %s ADD COLUMN %s TEXT DEFAULT \"N/A\""
    List("phone_cost_info", "patient_ask_num").foreach(addKey =>
      sqlCon.prepareStatement(sqlAddColumn.format(tbName, addKey)).executeUpdate())
    def receive = {
      case DtbsUpdate(rowElem) =>
        if (!(rowElem.isEmpty)) sqlCon.prepareStatement(sqlUpdate.format(tbName, rowElem("phone_cost_info"), rowElem("patient_ask_num"), rowElem("id"))).executeUpdate()
        entryInDtbs += 1
        Console.print("\b" * 6 + "% 6d".format(entryInDtbs)); Console.flush()
        context.actorFor("/user/alpha") ! DtbsUpdateRtn
      case DtbsQuery(sqlQuery) =>
        sender ! DtbsQueryRtn(sqlCon.prepareStatement(sqlQuery).executeQuery())
    }
  }

  // Dispatcher
  case object Run
  class Alpha extends Actor {
    val startTime: Double = System.currentTimeMillis
    val betaPool = context.actorOf(Props[Beta].withRouter(FromConfig()).withDispatcher("spyder-dispatcher"), "betaPool")
    var expect: Int = 0 // BetaAlphaWork 全部完成
    var count: Int = 0
    def receive = {
      case Run =>
        context.actorFor("/user/dtbs") ! DtbsQuery("SELECT id, can_ask, can_call, doctor_addr FROM doctor_basic")
      case DtbsQueryRtn(queryRst) =>
        while (queryRst.next) {
          expect += 1
          betaPool ! BetaWork((for (key <- List("id", "can_ask", "can_call", "doctor_addr")) yield (key -> queryRst.getString(key))).toMap)
        }
      case DtbsUpdateRtn =>
        count += 1
        if (count == expect) {
          Console.print("\n[info] AlphaTimer(s) :% 7.2f\n".format((System.currentTimeMillis - startTime) / 1000))
          context.system.shutdown()
        }
    }
  }

  //  RunMain
  def main(args: Array[String]) {
    val system = ActorSystem("ExtraSpyderSystem")
    val alpha = system.actorOf(Props[Alpha], name = "alpha")
    val dtbs = system.actorOf(Props(new Dtbs("haodf.db", "doctor_basic")), name = "dtbs")
    alpha ! Run
  }
}