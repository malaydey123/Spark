package Credit
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.Row


object saveastble {
  def main(args: Array[String]): Unit = {
    
  val sc = new SparkContext(new SparkConf().setAppName("Spark Credit"))
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  var dbName = args(0)
  
  sqlContext.sql(s"use $dbName")
  import sqlContext.implicits._
  
val crd_migteam_bal_a_code_prodid_mapping_temp_sub = sqlContext.sql("SELECT cpm.prod_id_group, cpm.prod_id, prds.a_code, cpm.product_type, cpm.cease_order,RANK () OVER (PARTITION BY cpm.prod_id ORDER BY cpm.a_code DESC) as max_start_date FROM crd_bal_bal_a_code_prodid_mapping cpm, crd_migteam_bal_a_code_products prds WHERE cpm.prod_id = prds.prod_id").toDF();

val crd_migteam_mbcd_crdt_dbt_inst_map_temp = sqlContext.sql("SELECT mcd_account_num, mcd_crdt_dbt_seq, mcd_classification, mcd_sub_classification FROM crd_bal_mbcd_crdt_dbt_inst_map bal, crd_migteam_mbcd_crdt_dbt_inst_map_temp_sub ai WHERE mcd_account_num = ai.account_num AND ai.flag = 'Y'").toDF();

val crd_cursor_step1 = sqlContext.sql("SELECT ai.customer_ref, ai.account_num,ai.product_line,adj.adjustment_seq,adj.adjustment_dat,adj.adjustment_txt, adj.adjustment_net_mny,adj.created_dtm, adj.dispute_seq, adj.bill_seq, adj.geneva_user_ora,tx.taxcode_name as TAXIDENTIFIER,adjt.adjustment_type_name,rc.revenue_code_name,bs.invoice_num,bs.bill_version,bs.actual_bill_dtm, IF (bs.bill_type_id = 7, 'T', 'F') credit_note_requested_flag,adj.cps_id FROM crd_subquery1 ai,crd_adjustment adj,crd_cpsadjtypetax adjtx,crd_taxsetcodes tsc,crd_taxcode tx,crd_adjustmenttype adjt,crd_revenuecode rc,crd_billsummary bs,crd_latest_bill latest_bill,crd_vl_c_crd_dft_end_date edate,crd_vl_extract_start_date sdate WHERE ai.flag = 'Y' AND ai.account_num = adj.account_num AND ADJ.ADJUSTMENT_TYPE_ID=adjtx.ADJUSTMENT_TYPE_ID AND ADJ.CPS_ID=adjtx.CPS_ID AND adjtx.TAX_SET_ID=tsc.TAX_SET_ID AND tsc.TAXcode_ID=tx.TAXcode_ID AND adj.adjustment_type_id = adjt.adjustment_type_id AND adjt.revenue_code_id = rc.revenue_code_id AND adj.account_num = bs.account_num AND adj.bill_seq = bs.bill_seq AND bs.actual_bill_dtm >= sdate.vl_extract_start_date_bs AND bs.actual_bill_dtm <= edate.vl_extract_end_date_bs AND adj.account_num = latest_bill.account_num AND adj.bill_seq = latest_bill.bill_seq AND bs.bill_version = latest_bill.bill_version").toDF();

val crd_dispute=sqlContext.sql("select * from crd_dispute").toDF();

val crd_cursor_step2 = crd_cursor_step1.join(crd_dispute,(crd_dispute("account_num")===crd_cursor_step1("account_num"))&&(crd_cursor_step1("dispute_seq")===crd_dispute("dispute_seq")),"left").select(crd_cursor_step1("customer_ref"),crd_cursor_step1("account_num"),crd_cursor_step1("product_line"),crd_cursor_step1("adjustment_seq"),crd_cursor_step1("adjustment_dat"),crd_cursor_step1("adjustment_txt"),crd_cursor_step1("adjustment_net_mny"),crd_cursor_step1("created_dtm"),crd_cursor_step1("dispute_seq"),crd_cursor_step1("bill_seq"),crd_cursor_step1("geneva_user_ora"),crd_cursor_step1("TAXIDENTIFIER"),crd_cursor_step1("adjustment_type_name"),crd_cursor_step1("revenue_code_name"),crd_cursor_step1("invoice_num"),crd_cursor_step1("bill_version"),crd_cursor_step1("actual_bill_dtm"),crd_cursor_step1("credit_note_requested_flag"),crd_cursor_step1("cps_id"),crd_dispute("product_seq"),crd_dispute("product_id")).toDF(); 

val crd_contractedpointofsupply=sqlContext.sql("select * from crd_contractedpointofsupply").toDF();


val crd_cursor_step3 = crd_cursor_step2.join(crd_contractedpointofsupply,(crd_contractedpointofsupply("cps_id")===crd_cursor_step2("cps_id")),"left").select(crd_cursor_step2("customer_ref"),crd_cursor_step2("account_num"),crd_cursor_step2("product_line"),crd_cursor_step2("adjustment_seq"),crd_cursor_step2("adjustment_dat"),crd_cursor_step2("adjustment_txt"),crd_cursor_step2("adjustment_net_mny"),crd_cursor_step2("created_dtm"),crd_cursor_step2("dispute_seq"),crd_cursor_step2("bill_seq"),crd_cursor_step2("geneva_user_ora"),crd_cursor_step2("TAXIDENTIFIER"),crd_cursor_step2("adjustment_type_name"),crd_cursor_step2("revenue_code_name"),crd_cursor_step2("invoice_num"),crd_cursor_step2("bill_version"),crd_cursor_step2("actual_bill_dtm"),crd_cursor_step2("credit_note_requested_flag"),crd_cursor_step2("cps_id"),crd_cursor_step2("product_seq"),crd_cursor_step2("product_id")).toDF(); 


val crd_cursor_step4 = crd_cursor_step3.join(crd_migteam_mbcd_crdt_dbt_inst_map_temp,(crd_cursor_step3("account_num")===crd_migteam_mbcd_crdt_dbt_inst_map_temp("mcd_account_num"))&&(crd_cursor_step3("adjustment_seq")===crd_migteam_mbcd_crdt_dbt_inst_map_temp("mcd_crdt_dbt_seq")),"left").select(crd_cursor_step3 ("customer_ref"),crd_cursor_step3("account_num"),crd_cursor_step3("product_line"),crd_cursor_step3("adjustment_seq"),crd_cursor_step3("adjustment_dat"),crd_cursor_step3("adjustment_txt"),crd_cursor_step3("adjustment_net_mny"),crd_cursor_step3("created_dtm"),crd_cursor_step3("dispute_seq"),crd_cursor_step3("bill_seq"),crd_cursor_step3("geneva_user_ora"),crd_cursor_step3("TAXIDENTIFIER"),crd_cursor_step3("adjustment_type_name"),crd_cursor_step3("revenue_code_name"),crd_cursor_step3("invoice_num"),crd_cursor_step3("bill_version"),crd_cursor_step3("actual_bill_dtm"),crd_cursor_step3("credit_note_requested_flag"),crd_cursor_step3("product_seq"),crd_cursor_step3("product_id"),when(crd_migteam_mbcd_crdt_dbt_inst_map_temp("mcd_classification")isNull,crd_cursor_step3("adjustment_type_name")).otherwise(crd_migteam_mbcd_crdt_dbt_inst_map_temp("mcd_classification" )).alias("credit_category"),when(crd_migteam_mbcd_crdt_dbt_inst_map_temp("mcd_sub_classification" )isNull,crd_cursor_step3("adjustment_type_name")).otherwise(crd_migteam_mbcd_crdt_dbt_inst_map_temp("mcd_sub_classification")).alias("credit_sub_category")).toDF(); 

val crd_disputeevent = sqlContext.sql("select * from crd_disputeevent").toDF();

val crd_dispute_view = crd_dispute.join(crd_disputeevent,(crd_dispute("account_num")===crd_disputeevent("account_num"))&&(crd_dispute("dispute_seq")===crd_disputeevent("dispute_seq")),"left").select(crd_dispute ("account_num"),crd_dispute ("dispute_seq"),crd_dispute ("bill_seq"),crd_dispute ("event_type_id")).toDF();

val crd_vl_dis_cnt = crd_dispute_view.join(crd_cursor_step4,crd_dispute_view("account_num")===crd_cursor_step4("account_num")&& (crd_dispute_view("dispute_seq")===crd_cursor_step4("dispute_seq"))&& (when (crd_dispute_view("bill_seq")isNull,"0").otherwise(crd_dispute_view("bill_seq"))<(crd_cursor_step4("bill_seq"))),"right").select (crd_cursor_step4("account_num"),when(crd_dispute_view("account_num").isNull,0).otherwise("1").alias("vl_dis_cnt")).toDF();


val crd_vl_dis_cnt_distinct = crd_vl_dis_cnt.distinct


val crd_eventtype = sqlContext.sql("select * from crd_eventtype").toDF();


val crd_vl_event_type_temp = crd_eventtype.join(crd_dispute_view,crd_eventtype("event_type_id")===(crd_dispute_view("event_type_id"))).select (crd_eventtype("event_type_name"),crd_eventtype("event_type_id"),crd_dispute_view("account_num"),crd_dispute_view("dispute_seq")).toDF();


val crd_vl_event_type = crd_vl_event_type_temp.join(crd_cursor_step4,crd_vl_event_type_temp("account_num")===crd_cursor_step4("account_num")&& (crd_vl_event_type_temp("dispute_seq")===crd_cursor_step4("dispute_seq"))).select (crd_vl_event_type_temp("event_type_name"),crd_vl_event_type_temp("event_type_id"),crd_vl_event_type_temp("account_num"),crd_vl_event_type_temp("dispute_seq")).toDF();


val crd_bal_balproductinstance = sqlContext.sql("select * from crd_bal_balproductinstance").toDF();

val crd_crm_asset_id_start_dat = crd_bal_balproductinstance.join(crd_cursor_step4,crd_bal_balproductinstance("customer_ref")===crd_cursor_step4("customer_ref")&&(crd_bal_balproductinstance("product_seq")===crd_cursor_step4("product_seq"))).select(crd_bal_balproductinstance("customer_ref"),crd_bal_balproductinstance("product_seq"),crd_bal_balproductinstance("start_dat")).toDF();


import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.Row

val crd_crm_asset_id_start_dat_group=crd_crm_asset_id_start_dat.groupBy("customer_ref","product_seq").agg(max("start_dat").alias("start_dat")).toDF();

val crd_vl_a_inst_id_1_temp = crd_bal_balproductinstance.join(crd_cursor_step4,crd_bal_balproductinstance("customer_ref")===crd_cursor_step4("customer_ref")&& crd_bal_balproductinstance("product_seq")===crd_cursor_step4("product_seq")).select (crd_bal_balproductinstance("crm_asset_id"),crd_bal_balproductinstance("customer_ref"),crd_bal_balproductinstance("product_seq"),crd_bal_balproductinstance("start_dat")).toDF();

val crd_vl_a_inst_id_1 = crd_vl_a_inst_id_1_temp.join(crd_crm_asset_id_start_dat_group,crd_vl_a_inst_id_1_temp("start_dat")===crd_crm_asset_id_start_dat_group("start_dat") && crd_vl_a_inst_id_1_temp("customer_ref")===crd_crm_asset_id_start_dat_group("customer_ref") && crd_vl_a_inst_id_1_temp("product_seq")===crd_crm_asset_id_start_dat_group("product_seq")).select (crd_vl_a_inst_id_1_temp("crm_asset_id"),crd_vl_a_inst_id_1_temp("customer_ref"),crd_vl_a_inst_id_1_temp("product_seq"),crd_vl_a_inst_id_1_temp("start_dat")).toDF();


val crd_bal_bal_productinstance = sqlContext.sql("select * from crd_bal_bal_productinstance").toDF();

val crd_prod_inst_id_start_dat = crd_bal_bal_productinstance.join(crd_cursor_step4,crd_bal_bal_productinstance("customer_ref")===crd_cursor_step4("customer_ref") && crd_bal_bal_productinstance("product_seq")===crd_cursor_step4("product_seq")).select(crd_bal_bal_productinstance("customer_ref"),crd_bal_bal_productinstance("product_seq"),crd_bal_bal_productinstance("start_dat")).toDF();

val crd_prod_inst_id_start_dat_group=crd_prod_inst_id_start_dat.groupBy("customer_ref","product_seq").agg(max("start_dat").alias("start_dat")).toDF();


val crd_vl_a_inst_id_2_temp = crd_bal_bal_productinstance.join(crd_cursor_step4,crd_bal_bal_productinstance("customer_ref")===crd_cursor_step4("customer_ref") && crd_bal_bal_productinstance("product_seq")===crd_cursor_step4("product_seq")).select (crd_bal_bal_productinstance("prod_inst_id"),crd_bal_bal_productinstance("customer_ref"),crd_bal_bal_productinstance("product_seq"),crd_bal_bal_productinstance("start_dat")).toDF();

crd_vl_a_inst_id_2_temp.registerTempTable("crd_vl_a_inst_id_2_temp_table")

crd_prod_inst_id_start_dat_group.registerTempTable("crd_prod_inst_id_start_dat_group_table")


val crd_vl_a_inst_id_2 = sqlContext.sql("select NVL (SUBSTR (ev.prod_inst_id, 1, ( INSTR (ev.prod_inst_id, ':') - 1 ) ), ev.customer_ref ) as vl_a_inst_id,ev.customer_ref,ev.product_seq,ev.start_dat  from crd_vl_a_inst_id_2_temp_table ev,crd_prod_inst_id_start_dat_group_table dt where ev.start_dat = dt.start_dat AND ev.customer_ref = dt.customer_ref AND ev.product_seq = dt.product_seq").toDF();

scala.util.Try(sqlContext.dropTempTable("crd_vl_a_inst_id_2_temp_table"))

scala.util.Try(sqlContext.dropTempTable("crd_prod_inst_id_start_dat_group_table"))


crd_cursor_step4.registerTempTable("crd_cursor_step4_table")

val crd_vl_tariff_id_charge_number = sqlContext.sql("SELECT MAX (charge_number) as charge_number, account_num FROM crd_billproductcharge GROUP BY account_num").toDF();

crd_vl_tariff_id_charge_number.registerTempTable("crd_vl_tariff_id_charge_number_table")

val crd_vl_tariff_id=sqlContext.sql("SELECT a.tariff_id as vl_tariff_id, a.charge_number, a.product_id, a.product_seq, a.charge_seq, TO_DATE (c.actual_bill_dtm) as actual_bill_dtm FROM crd_billproductcharge a, crd_vl_tariff_id_charge_number_table b, crd_billsummary c, crd_cursor_step4_table j WHERE a.charge_number = b.charge_number AND a.product_id = j.product_id AND a.product_seq = j.product_seq AND a.charge_seq = c.charge_seq AND TO_DATE (c.actual_bill_dtm) = j.actual_bill_dtm").toDF();

scala.util.Try(sqlContext.dropTempTable("crd_vl_tariff_id_charge_number_table"))


val crd_bal_balselectorseqinstance = sqlContext.sql("select * from crd_bal_balselectorseqinstance").toDF();

crd_bal_balselectorseqinstance.registerTempTable("crd_bal_balselectorseqinstance_table")

val crd_vl_c_crd_dft_end_date = sqlContext.sql("select * from crd_vl_c_crd_dft_end_date").toDF();

crd_vl_c_crd_dft_end_date.registerTempTable("crd_vl_c_crd_dft_end_date_table")

val crd_selector_attr_seq = sqlContext.sql("SELECT bssi.selector_attr_seq FROM crd_bal_balselectorseqinstance_table bssi, crd_cursor_step4 j, crd_vl_c_crd_dft_end_date_table ed WHERE bssi.customer_ref = j.customer_ref AND bssi.product_seq = j.product_seq AND bssi.account_num = j.account_num AND NVL (bssi.end_dat, ed.vl_extract_end_date) >= ed.vl_extract_end_date").toDF();



scala.util.Try(sqlContext.dropTempTable("crd_bal_balselectorseqinstance_table"))

scala.util.Try(sqlContext.dropTempTable("crd_vl_c_crd_dft_end_date_table"))


crd_vl_tariff_id.registerTempTable("crd_vl_tariff_id_table")

crd_selector_attr_seq.registerTempTable("crd_selector_attr_seq_table")


val crd_balsiebeltogenevaprodmapping_required = sqlContext.sql("SELECT bstgpm.product_id as vl_product_id, bstgpm.attribute_1 as vl_attribute_1, bstgpm.attribute_3 as vl_attribute_3, bstgpm.gen_prod_id, bstgpm.gen_trf_id, bstgpm.selector_attr_seq FROM crd_bal_balsiebeltogenevaprodmapping bstgpm, crd_selector_attr_seq_table saq, crd_vl_tariff_id_table ti, crd_cursor_step4_table j WHERE bstgpm.gen_prod_id = j.product_id AND bstgpm.gen_trf_id = ti.vl_tariff_id AND bstgpm.selector_attr_seq = saq.selector_attr_seq").toDF();


scala.util.Try(sqlContext.dropTempTable("crd_selector_attr_seq_table"))


crd_balsiebeltogenevaprodmapping_required.registerTempTable("crd_balsiebeltogenevaprodmapping_required_table")


val crd_s_code_vl_product_code = sqlContext.sql("SELECT bscpm.s_code as vl_product_code, bscpm.prod_id, bscpm.product_type FROM crd_bal_s_code_prodid_mapping bscpm, crd_balsiebeltogenevaprodmapping_required_table gpm WHERE bscpm.prod_id = gpm.vl_product_id AND bscpm.product_type = gpm.vl_attribute_1").toDF();


scala.util.Try(sqlContext.dropTempTable("crd_balsiebeltogenevaprodmapping_required_table"))


val crd_a_code_product_id = sqlContext.sql("SELECT sgpm.product_id FROM crd_bal_bal_siebeltogenevaprodmapping sgpm, crd_vl_tariff_id_table ti, crd_cursor_step4_table j WHERE sgpm.gen_prod_id = j.product_id AND sgpm.gen_trf_id = ti.vl_tariff_id").toDF();

crd_a_code_product_id.registerTempTable("crd_a_code_product_id_table")

val crd_a_code = sqlContext.sql("SELECT pmt.a_code as vl_product_code FROM crd_migteam_bal_a_code_prodid_mapping_temp pmt, crd_a_code_product_id_table apid WHERE pmt.prod_id = apid.product_id").toDF();


scala.util.Try(sqlContext.dropTempTable("crd_a_code_product_id_table"))



val crd_vl_billinginternal_prodcode_1 = sqlContext.sql("SELECT sgpm.product_id as vl_billinginternal_prodcode, sgpm.gen_prod_id, sgpm.gen_trf_id FROM crd_bal_balsiebeltogenevaprodmapping sgpm, crd_vl_tariff_id_table ti, crd_cursor_step4_table j WHERE sgpm.gen_prod_id = j.product_id AND sgpm.gen_trf_id = ti.vl_tariff_id").toDF();


val crd_vl_billinginternal_prodcode_2 = sqlContext.sql("SELECT sgpm.product_id as vl_billinginternal_prodcode, sgpm.gen_prod_id, sgpm.gen_trf_id FROM crd_bal_bal_siebeltogenevaprodmapping sgpm, crd_vl_tariff_id_table ti, crd_cursor_step4_table j WHERE sgpm.gen_prod_id = j.product_id AND sgpm.gen_trf_id = ti.vl_tariff_id").toDF();



crd_dispute_view.registerTempTable("crd_dispute_view_table")

val crd_vl_d_inv_id_inv_ver = sqlContext.sql("SELECT bs.invoice_num as vl_d_inv_id, bs.bill_version as vl_d_inv_ver, d.bill_seq, d.account_num, bs.bill_seq as bill_seq_2, bs.account_num as account_num_2, d.dispute_seq FROM crd_dispute_view_table d, crd_billsummary bs, crd_cursor_step4_table j WHERE d.bill_seq < j.bill_seq AND d.account_num = j.account_num AND bs.bill_seq = j.bill_seq AND bs.account_num = j.account_num AND d.dispute_seq = j.dispute_seq AND bs.bill_version = j.bill_version").toDF();


scala.util.Try(sqlContext.dropTempTable("crd_dispute_view_table"))


scala.util.Try(sqlContext.dropTempTable("crd_cursor_step4_table"))


val crd_transition_table_1 = crd_cursor_step4.join(crd_vl_event_type,crd_vl_event_type("account_num")===crd_cursor_step4("account_num")&& crd_vl_event_type("dispute_seq")===crd_cursor_step4("dispute_seq"),"left").select (crd_cursor_step4.col("*"),crd_vl_event_type("event_type_name")).toDF();


val crd_transition_table_2 = crd_transition_table_1.join(crd_vl_a_inst_id_1,crd_vl_a_inst_id_1("customer_ref")===crd_transition_table_1("customer_ref") && crd_vl_a_inst_id_1("product_seq")===crd_transition_table_1("product_seq"),"left").select (crd_transition_table_1.col("*"),crd_vl_a_inst_id_1("crm_asset_id").alias("vl_a_inst_id")).toDF();


val crd_transition_table_3_1 = crd_transition_table_2.join(crd_vl_a_inst_id_2,crd_vl_a_inst_id_2("customer_ref")===crd_transition_table_2("customer_ref")&& crd_vl_a_inst_id_2("product_seq")===crd_transition_table_2("product_seq") && crd_transition_table_2("vl_a_inst_id").isNull,"left").select (crd_transition_table_2("customer_ref"),crd_transition_table_2("account_num"),crd_transition_table_2("product_line"),crd_transition_table_2("adjustment_seq"),crd_transition_table_2("adjustment_dat"),crd_transition_table_2("adjustment_txt"),crd_transition_table_2("adjustment_net_mny"),crd_transition_table_2("created_dtm"),crd_transition_table_2("dispute_seq"),crd_transition_table_2("bill_seq"),crd_transition_table_2("geneva_user_ora"),crd_transition_table_2("TAXIDENTIFIER"),crd_transition_table_2("adjustment_type_name"),crd_transition_table_2("revenue_code_name"),crd_transition_table_2("invoice_num"),crd_transition_table_2("bill_version"),crd_transition_table_2("actual_bill_dtm"),crd_transition_table_2("credit_note_requested_flag"),crd_transition_table_2("product_seq"),crd_transition_table_2("product_id"),crd_transition_table_2("credit_category"),crd_transition_table_2("credit_sub_category"),crd_transition_table_2("event_type_name"),when(crd_vl_a_inst_id_2("vl_a_inst_id")isNull,crd_transition_table_2("customer_ref")).otherwise(crd_vl_a_inst_id_2("vl_a_inst_id")).alias("vl_a_inst_id")).toDF();

crd_transition_table_2.registerTempTable("crd_transition_table_2_table")

val crd_transition_table_3_2 = sqlContext.sql("select * from crd_transition_table_2_table WHERE vl_a_inst_id is NOT NULL").toDF();


scala.util.Try(sqlContext.dropTempTable("crd_transition_table_2_table"))


val crd_transition_table_3 =crd_transition_table_3_1.unionAll(crd_transition_table_3_2).toDF();


val crd_transition_table_4 = crd_transition_table_3.join(crd_vl_tariff_id,crd_vl_tariff_id("product_id")===crd_transition_table_3("product_id")&& crd_vl_tariff_id("product_seq")===crd_transition_table_3("product_seq")&& crd_vl_tariff_id("actual_bill_dtm")===crd_transition_table_3("actual_bill_dtm"),"left").select (crd_transition_table_3.col("*"),crd_vl_tariff_id("vl_tariff_id")).toDF();


val crd_transition_table_5 =crd_transition_table_4.join(crd_balsiebeltogenevaprodmapping_required,crd_balsiebeltogenevaprodmapping_required("gen_prod_id")===crd_transition_table_4("product_id"),"left").select(crd_transition_table_4.col("*"),crd_balsiebeltogenevaprodmapping_required("vl_product_id"),crd_balsiebeltogenevaprodmapping_required("vl_attribute_1"),crd_balsiebeltogenevaprodmapping_required("vl_attribute_3")).toDF();


crd_transition_table_5.registerTempTable("crd_transition_table_5_table")

val crd_transition_table_6 = sqlContext.sql("SELECT j.* , CASE  WHEN j.vl_product_id = 'GEA Cablelink' THEN CASE WHEN j.vl_attribute_3 = '1Gb' THEN 'R0200901' WHEN j.vl_attribute_3 = '10Gb' THEN 'R6100077' ELSE 'R0200901' END ELSE pc.s_code END AS vl_product_code FROM crd_transition_table_5_table j LEFT OUTER JOIN crd_bal_s_code_prodid_mapping pc ON (pc.prod_id = j.vl_product_id AND pc.product_type = j.vl_attribute_1)").toDF();


scala.util.Try(sqlContext.dropTempTable("crd_transition_table_5_table"))


crd_transition_table_6.registerTempTable("crd_transition_table_6_table")

crd_a_code.registerTempTable("crd_a_code_table")

val crd_transition_table_7_1 =sqlContext.sql("SELECT j.customer_ref, j.account_num, j.product_line, j.adjustment_seq, j.adjustment_dat,j.adjustment_txt, j.adjustment_net_mny,j.created_dtm, j.dispute_seq, j.bill_seq,j.geneva_user_ora,j.TAXIDENTIFIER,j.adjustment_type_name,j.revenue_code_name,j.invoice_num, j.bill_version,j.actual_bill_dtm,j.credit_note_requested_flag,j.product_seq, j.product_id,j.credit_category,j.credit_sub_category,j.event_type_name,j.vl_a_inst_id,j.vl_tariff_id,j.vl_product_id,j.vl_attribute_1,j.vl_attribute_3,case when inid.vl_product_code is null then j.product_id else inid.vl_product_code end as vl_product_code FROM crd_transition_table_6_table j LEFT OUTER JOIN crd_a_code_table inid ON (j.vl_product_code is NULL)").toDF();


val crd_transition_table_7_2 =sqlContext.sql("SELECT * FROM crd_transition_table_6_table WHERE vl_a_inst_id is NOT NULL").toDF();

scala.util.Try(sqlContext.dropTempTable("crd_transition_table_6_table"))

scala.util.Try(sqlContext.dropTempTable("crd_a_code_table"))


val crd_transition_table_7 =crd_transition_table_7_1.unionAll(crd_transition_table_7_2).toDF();


val crd_transition_table_8 = crd_transition_table_7.join(crd_vl_billinginternal_prodcode_1,crd_vl_billinginternal_prodcode_1("gen_prod_id")===crd_transition_table_7("product_id") && crd_vl_billinginternal_prodcode_1("gen_trf_id")===crd_transition_table_7("vl_tariff_id"),"left").select(crd_transition_table_7.col("*"),crd_vl_billinginternal_prodcode_1("vl_billinginternal_prodcode")).toDF();

crd_transition_table_8.write.mode("overwrite").saveAsTable("crd_transition_table_8_table") 

crd_vl_billinginternal_prodcode_2.write.mode("overwrite").saveAsTable("crd_vl_billinginternal_prodcode_2_table")

crd_vl_dis_cnt_distinct.write.mode("overwrite").saveAsTable("crd_vl_dis_cnt_distinct_table")

crd_vl_d_inv_id_inv_ver.write.mode("overwrite").saveAsTable("crd_vl_d_inv_id_inv_ver_table")

  }
}
