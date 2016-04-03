/* [创建表名] doctor_detail
 * [运行时间] 1200 sec
 * [依赖表名] doctor_basic(*)
 * [字段内容]
 * id                          >>  Auto
 * hospital_name               >>  医生 所属医院
 * office_name                 >>  医生 所属科室
 * doctor_name                 >>  医生 姓名
 * info_addr                   >>  医生 好大夫主页
 * title_name                  >>  医生 职称 学历
 * recommend_index             >>  医生 患者推荐热度
 * doctor_addr                 >>  医生 个人主页
 * can_change                  >>  医生 能否转诊
 * can_call                    >>  医生 能否通电话
 * can_ask                     >>  医生 能否咨询
 * appoint_if                  >>  医生 预约条件
 * appoint_time                >>  医生 预约时间
 * suc_appoint_num             >>  医生 成功预约人数
 * phone_cost_info             >>  医生 电话咨询收费标准
 * patient_ask_num             >>  医生 患者网上咨询服务总人数
 * thank_letter_num            >>  医生 感谢信数量
 * gift_num                    >>  医生 礼物数量
 * intro_doctor                >>  医生 执业经历
 * good_at                     >>  医生 擅长
 * query_num                   >>  医生 患者提问数
 * query_reply_num             >>  医生 回答患者提问数
 * patient_vote                >>  医生 患者投票情况
 * performance_rate            >>  医生 疗效满意度
 * attitude_rate               >>  医生 态度满意度
 * love_num                    >>  医生 爱心值
 * contri_value                >>  医生 贡献值
 * stat_view_num               >>  医生 个人网站总访问次数
 * stat_yester_view_num        >>  医生 个人网站昨日访问次数
 * stat_article_num            >>  医生 总文章篇数
 * stat_patient_num            >>  医生 总患者人数
 * stat_yester_postcontact_num >>  医生 昨日诊后报到患者人数
 * stat_wechat_postcontact_num >>  医生 微信诊后报到患者人数
 * stat_total_postcontact_num  >>  医生 总诊后报到患者人数
 * stat_rate_num               >>  医生 患者投票数
 * stat_thanks_num             >>  医生 感谢信数
 * stat_gifts_num              >>  医生 心意礼物数
 * stat_last_login_date        >>  医生 个人网站上次在线时间
 * stat_register_date          >>  医生 个人网站开通时间
 */

object DoctorDetailSpyder {
  import akka.actor.{ Actor, Props, ActorSystem }
  import akka.routing.FromConfig
  import org.jsoup.Jsoup
  import java.sql.ResultSet
  import java.util.regex.Pattern
  import org.jsoup.nodes.Document
  import org.jsoup.nodes.Element
  type RowElem = Map[String, String]

  // Html Worker
  case class BetaAlphaWork(rowElem: RowElem)
  case class BetaBetaWork(rowElem: RowElem)
  class Beta extends Actor {
    val statInfoMap = Map("stat_view_num" -> "总 访 问", "stat_yester_view_num" -> "昨日访问", "stat_article_num" -> "总 文 章", "stat_patient_num" -> "总 患 者", "stat_yester_postcontact_num" -> "昨日诊后报到患者", "stat_wechat_postcontact_num" -> "微信诊后报到患者", "stat_total_postcontact_num" -> "总诊后报到患者", "stat_rate_num" -> "患者投票", "stat_thanks_num" -> "感 谢 信", "stat_gifts_num" -> "心意礼物", "stat_last_login_date" -> "上次在线", "stat_register_date" -> "开通时间")
    val docAddrInfoKeys = statInfoMap.keySet.toList ++ List("love_num", "contri_value") // 从doctor_addr获取的全部键值
    def receive = {
      case BetaAlphaWork(rowElem) =>
        val infoRowElem = parseInforAddr(rowElem)
        if (infoRowElem("doctor_addr") == "N/A")
          context.actorFor("/user/dtbs") ! DtbsWrite(infoRowElem ++ docAddrInfoKeys.map(_ -> "N/A").toMap)
        else
          context.actorFor("/user/alpha/betaPool") ! BetaBetaWork(infoRowElem)
      case BetaBetaWork(rowElem) =>
        context.actorFor("/user/dtbs") ! DtbsWrite(parseDoctorAddr(rowElem))
    }

    val QUERY_PATT = Pattern.compile("患者提问:(\\d+)问.*?本人已回复:(\\d+)问")
    val RATE_PATT = Pattern.compile("疗效.*?(\\d+?%).*?满意.*?态度.*?(\\d+?%).*?满意")
    def parseInforAddr(rowElem: RowElem): RowElem = {
      val infoAddrDoc = Jsoup.parse(Toolkit.getHtml(rowElem("info_addr")))
      val scriptList = infoAddrDoc.select("script").toArray.toList.asInstanceOf[List[Element]]
      val doctorInfoDoc = Jsoup.parse(findBigPipeById(infoAddrDoc, scriptList, "bp_doctor_about"))
      val thankLetterElem = doctorInfoDoc.select("a[href~=.*?ganxiexin/1.htm]:contains(感谢信) span").first
      val giftNumElem = doctorInfoDoc.select("a[href~=.*?showPresent]:contains(礼物) span").first
      val goodAtElem = doctorInfoDoc.getElementById("full")
      val brefIntroElem = doctorInfoDoc.getElementById("full_DoctorSpecialize")
      var (queryNum, queryReplyNum) = parseMatchTwo(QUERY_PATT, doctorInfoDoc.select("table.doct_data_xxzx").first)
      val doctorVoteDoc = Jsoup.parse(findBigPipeById(infoAddrDoc, scriptList, "bp_doctor_getvote"))
      val voteElem = doctorVoteDoc.select("div.ltdiv").first
      var (perforRate, attRate) = parseMatchTwo(RATE_PATT, doctorVoteDoc.select("div.rtdiv").first)
      rowElem ++ Map(
        "thank_letter_num" -> (if (thankLetterElem == null) "N/A" else thankLetterElem.text),
        "gift_num" -> (if (giftNumElem == null) "N/A" else giftNumElem.text),
        "intro_doctor" -> (if (goodAtElem == null) "N/A" else goodAtElem.text.replaceAll("\\\\[tnr]", "").replaceAll("<< 收起", "").replaceAll("\\s+", "")),
        "good_at" -> (if (brefIntroElem == null) "N/A" else brefIntroElem.text.replaceAll("\\\\[tnr]", "").replaceAll("\\s+", "")),
        "query_num" -> queryNum,
        "query_reply_num" -> queryReplyNum,
        "patient_vote" -> (if (voteElem == null) "N/A" else voteElem.text.replaceAll("\\\\[tnr]", "").replaceAll("近两年暂无患者投票", "").replaceAll("查看两年前患者投票", "两年前").replaceAll("\\s+", "")),
        "performance_rate" -> perforRate,
        "attitude_rate" -> attRate)
    }
    def parseDoctorAddr(rowElem: RowElem): RowElem = {
      val docAddrDoc = Jsoup.parse(Toolkit.getHtml(rowElem("doctor_addr")))
      val docInfoUL = docAddrDoc.select("ul.doc_info_ul1").first
      val loveNumElem = if (docInfoUL == null) null else docInfoUL.select("a[title~=爱心值]").first
      val statElemList = docAddrDoc.select("ul.space_statistics").first
      val statElemMap = if (statElemList == null) statInfoMap.keySet.toList.map(key => key -> "N/A").toMap
        else statInfoMap.keySet.toList.map(key => key -> {
          var res = ""
          try { res = statElemList.select("li:contains(%s)".format(statInfoMap(key))).first.select("span[class=\"orange1 pr5\"]").first.text }
          catch { case _ => res = "N/A" }   
          res       
        }).toMap
      rowElem ++ statElemMap ++ Map(
        "love_num" -> (if (loveNumElem == null) "N/A" else Toolkit.extractNumFromString(loveNumElem.attr("title")).toString),
        "contri_value" -> (if (docInfoUL == null) "N/A" else docInfoUL.select("a[href~=scoreinfo_doctor]").first.text))
    }
    val CONTENT_PATT = Pattern.compile("\"content\":\"(.+?[^\\\\])\"")
    def findBigPipeById(wholeHtmlDoc: Document, scriptList: List[Element], id: String): String = {
      val chosenScript = scriptList.filter(_.data.matches(".+\"id\":\"%s\".+".format(id)))
      if (chosenScript.length == 0) {
        val idItem = wholeHtmlDoc.getElementById(id)
        if (idItem != null) idItem.toString
        else "<html></html>" // 返回空
      }
      else {
        val matcher = CONTENT_PATT.matcher(chosenScript(0).data)
        if (matcher.find) // 解码Unicode-Escape, 且恢复html格式
          Toolkit.decodeUnicodeEscape(matcher.group(1)).replaceAll("\\\\/", "/").replaceAll("\\\\\"", "\"")
        else
          throw new Exception("Javascript BigPipe Cannot Find By Id [%s]".format(id))
      }
    }
    def parseMatchTwo(pattern: Pattern, elem: Element): (String, String) = {
      var v1, v2 = "N/A"
      if (elem != null) {
        val matcher = pattern.matcher(elem.text.replaceAll("\\\\[tnr]", ""))
        if (matcher.find) {
          v1 = matcher.group(1)
          v2 = matcher.group(2)
        }
      }
      (v1, v2)
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
    var expect: Int = 0 // BetaAlphaWork 全部完成
    var count: Int = 0
    def receive = {
      case Run =>
        context.actorFor("/user/dtbs") ! DtbsQuery("SELECT * FROM doctor_basic")
      case DtbsQueryRtn(queryRst) =>
        while (queryRst.next) {
          expect += 1
          betaPool ! BetaAlphaWork((for (
            key <- List("hospital_name", "office_name", "doctor_name", "info_addr", "title_name", "recommend_index", "doctor_addr", "can_change", "can_call", "can_ask", /* 原doctor_basic中字段 */
              "appoint_if", "appoint_time", "suc_appoint_num", "phone_cost_info", "patient_ask_num" /* 新增字段 */ )
          ) yield (key -> queryRst.getString(key))).toMap)
        }
      case DtbsWriteRtn =>
        count += 1
        if (count == expect) {
          Console.print("\n[info] AlphaTimer(s) :% 7.2f\n".format((System.currentTimeMillis - startTime) / 1000))
          context.system.shutdown()
        }
    }
  }

  //  RunMain
  def main(args: Array[String]) {
    val system = ActorSystem("DoctorDetailSpyderSystem")
    val alpha = system.actorOf(Props[Alpha], name = "alpha")
    val dtbs = system.actorOf(Props(new Dtbs("haodf.db", "doctor_detail")), name = "dtbs")
    alpha ! Run
  }
}