/* [创建表名] doctor_summary
 * [运行时间] 
 * [字段内容]
 * id                     >>  Auto
 * doctor_name            >>  医生 姓名
 * office_name            >>  医生 所属科室
 * hospital_name          >>  医生 所属医院
 * duration_day           >>  医生 时间跨度，数据抓取日期 - 个人网站开启日期
 * can_ask                >>  医生 能否咨询
 * ask_num_div            >>  医生 咨询患者总数/时间跨度
 * avg_ask_total_num      >>  医生 平均 总对话数
 * avg_ask_doctor_percent >>  医生 平均 医生对话数/总对话数 
 * can_call               >>  医生 能否通电话
 * min_phone_cost         >>  医生 最小电话咨询费用
 * can_appoint            >>  医生 能否预约转诊
 * sum_appoint_time       >>  医生 可预约时间段SUM(全天=2, 上/下午=1)
 * appoint_num_div        >>  医生 预约成功人数/时间跨度
 * gift_num               >>  医生 礼物数
 * thank_letter_num       >>  医生 感谢信数
 * vote_num               >>  医生 投票数
 * article_num_div        >>  医生 文章总量/时间跨度
 * article_read_num_div   >>  医生 文章阅读人数/时间跨度
 */

object ComputeIndex {
  import java.sql.DriverManager
  import java.sql.Connection
  type RowElem = Map[String, String]

  // calculate data index
  def calcIndex(beginIndexMap: RowElem, haodfCon: Connection): RowElem = {
    beginIndexMap
  }

  //  RunMain
  var hasTable = false    // has created table needed ?
  val tbName = "doctor_summary"
  def main(args: Array[String]) {
    // config database
    val haodfCon = DriverManager.getConnection("jdbc:sqlite:%s".format("haodf.db"))
    val indexCon = DriverManager.getConnection("jdbc:sqlite:%s".format("index.db"))
    indexCon.setAutoCommit(false)
    // calculate index
    val queryRst = haodfCon.prepareStatement("SELECT * FROM doctor_basic where id <= 10").executeQuery()
    while (queryRst.next) {
      val beginIndexMap = List("doctor_name", "hospital_name", "office_name").map(key => key -> queryRst.getString(key)).toMap
      val finalIndexMap = calcIndex(beginIndexMap, haodfCon)
      // write into database
      if (!hasTable) hasTable = Toolkit.createDtbsTable(indexCon, tbName, finalIndexMap.keySet.toList)
      Toolkit.writeDtbsEntry(indexCon, tbName, finalIndexMap)
    }
    // close database connection
    indexCon.commit()
    indexCon.close()
    haodfCon.close()
  }
}