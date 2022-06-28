package dataingest

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.{BufferedWriter, FileNotFoundException, FileWriter, _}

import dataingest.ReadfromDB.{logger, source_cnt}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.apache.spark.sql.SparkSession
//import spark.sqlContext.implicits._
import org.apache.hadoop.fs.{ FSDataInputStream, FileSystem }
import java.io.FileInputStream
import java.io.FileReader
import java.sql.{ Connection, ResultSet, SQLException, Statement }
import java.util.Properties
import javax.mail.Message
import javax.mail.Session
import javax.mail.Transport
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage
import org.apache.hadoop.conf.Configuration
import java.util.Properties
import java.io.FileInputStream

object Attunity_Recon {

  var spark: org.apache.spark.sql.SparkSession = null
  val logger = LogManager.getLogger(getClass.getName)
  var log_props: Properties = null
  var master: String = null
  var JOB_ID: String = null
  var PROJ_NAME: String = null
  var MODE: String = ""
  var LOWER_LIMIT: String = null
  var UPPER_LIMIT: String = null
  var DL_path: String = ""
  var Source_Conn_Path: String = ""
  var Target_Conn_Path: String = ""
  var Source_cnt: String = ""
  var Source_cnt_temp: String = ""
  var Source_cnt_order: String = ""
  var Target_cnt: String = ""
  var Diff_cnt: Int = 0
  var Temp_file: String = ""
  var Tables_list: String = ""
  var Tables_list_tgtwise: String = ""
  var Audit_list: String = ""
  var Target_tbl_list: String = ""
  var i: Int = 0
  var j: Int = 0
  var FREQ: Int = 240
  var START_TIME: Calendar = null
  var format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  var format_temp = new java.text.SimpleDateFormat("YYYYMMddHHmmss")
  var LOG_ROOT_DIR: String = null
  var home_var = "cmidevanalyticslake"
  var base_path: String = "/mnt/" + home_var + "/code/databricks/enterprise/"
  var ADLS_URL: String = ""
  var AUDIT_USERNAME: String = ""
  var AUDIT_PASSWD: String = ""
  var AUDIT_CONN_STRING: String = ""
  var AUDIT_DBTYPE: String = ""
  var AUDIT_HOSTNAME: String = ""
  var AUDIT_CONN_DBNAME: String = ""
  var AUDITPORT = 1433
  var AUDIT_RECON_TBL: String = ""
  var AUDIT_CNTRL_TBL: String = ""
  var propertyData: Properties = null
  var CONN_STR: String = ""
  var USERNAME: String = ""
  var SRC_DBTYPE: String = ""
  var INC_MODE: String = ""
  var PWD: String = ""
  var SCHEMA_NAME: String = ""
  var HIVE_DBNAME: String = ""
  var TARGET_DBTYPE: String = ""
  var fis_logs: FileInputStream = null
  val Prop: Properties = new Properties
  var fs: FileSystem = null
  var InputStream: FileInputStream = null
  var DSInputStream: FSDataInputStream = null
  var TGT_CONN_STR: String = ""
  var TGT_USERNAME: String = ""
  var TGT_PWD: String = ""
  var TGT_SCHEMA_NAME: String = ""
  var TGT_TBLNM_PREFIX: String = ""
  var TGT_TBLNM_SUFFIX: String = ""
  var DEL_COL: String = ""
  var ENABLE_DSN: String = ""
  var DSN_CONDITION: String = ""
  var DB_NAME_ATT: String = ""
  var DB_CHECK_ATT: String = "N"
  var DEL_WHERE: String = ""
  var LAST_UPD_VAL: String = ""
  var LAST_UPD_VAL_UPDATED: String = ""
  var Recon_Start_datetime: String = null
  var AUDIT_COL: String = ""
  var LAST_UPD_VAL_LL: String = ""
  var LAST_UPD_VAL_UL: String = ""
  var HIVE_WHERE: String = ""
  var SRC_RECON_WHERE: String = ""
  var TGT_RECON_WHERE: String = ""
  var src_count_query: String = ""
  var tgt_count_query: String = ""
  var targetData: Properties = null
  var srccountData: Properties = null
  var fintargetData: Properties = null
  var target_tbl: String = ""
  var DSN_WHERE: String = ""
  var FIN_TARGET: String = ""
  var res: ResultSet = null
  var conn: Connection = null
  var stmnt: Statement = null
  var Recon_status: String = ""
  var driver1: String = ""
  var UniqueID: String = ""
  var UPD_VAL: String = ""
  var insertquery: String = ""
  var Job_Start_datetime: String = null
  var last_upd_val_query: String = ""
  var Source_query: String = ""
  var Target_query: String = ""
  var lines = new StringBuilder
  val config: Configuration = new Configuration
  var fis: FileInputStream = null
  var fis_tgt: FileInputStream = null
  var fis_tgt_final: FileInputStream = null
  var fis_srccnt: FileInputStream = null
  var source_conn_file: FileInputStream = null
  var target_conn_file: FileInputStream = null
  var props: Properties = null

  var to = "rj242@cummins.com"
  var host = "10.208.0.104"
  var port = "25"
  var from = "pa558@cummins.com"
  val properties = new Properties()

  def main(args: Array[String]): Unit = {

    props = new Properties

    try {

      if (!InitializeObjects(args)) {
        logger.error("Error initialising objects..")
      }

      initializeLog4j()
      logger.info("Program started...")
      val ClusterStarttime = Calendar.getInstance().getTime()
      logger.info("DataBricks Cluster started and jar picked: " + ClusterStarttime)

      spark = SparkSession.builder().appName("ATTUNITY_RECON").master(master).enableHiveSupport().getOrCreate()

      logger.info("SparkSession: " + spark.sparkContext)
      logger.info("DataBricks Job Details: " + spark.conf.get("spark.databricks.clusterUsageTags.clusterName"))

      logger.info("Read Connection file..")
      if (!readConnectionFile()) {
        logger.error("ERROR reading Connection file..")
      }

      if (TARGET_DBTYPE.toUpperCase.contains("ORACLE") || TARGET_DBTYPE.toUpperCase.contains("SQLSERVER") || TARGET_DBTYPE.toUpperCase.contains("AZURE_SQLDB")) {
        logger.info("Read Target Connection file..")
        if (!readTargetConnectionFile()) {
          logger.error("ERROR reading Target Connection file..")
        }
      }

      if (!GetMaxDate()) {
        logger.error("ERROR getting max date from SQL Metastore..")
      }

      fs = FileSystem.get(URI.create(base_path), config)
      val rddFromFile = spark.sparkContext.textFile(Tables_list)

      if (TARGET_DBTYPE.toUpperCase.equals("HIVE") || TARGET_DBTYPE.toUpperCase.equals("ORACLE") || TARGET_DBTYPE.toUpperCase.contains("SQLSERVER") || TARGET_DBTYPE.toUpperCase.contains("AZURE_SQLDB")) {
        rddFromFile.collect().foreach(f => {
          logger.info("Table name is - " + f)

          props = new Properties
          fis = new FileInputStream(Audit_list)
          props.load(fis)
          if (props == null) {
            logger.info("Props is null - Audit_list")
            return false
          }
          AUDIT_COL = props.getProperty(f)
          logger.info("Audit column is " + AUDIT_COL)

          format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
          val now = Calendar.getInstance().getTime()
          Recon_Start_datetime = format.format(now)

          if (AUDIT_COL != "" && AUDIT_COL != null) {
            if (SRC_DBTYPE.toUpperCase.equals("AS400DB2")) {
              HIVE_WHERE = "where " + AUDIT_COL + " <= " + LAST_UPD_VAL
            } else {
              HIVE_WHERE = "where unix_timestamp(" + AUDIT_COL + ",'yyyy-MM-dd HH24:mm:ss') <= unix_timestamp('" + LAST_UPD_VAL + "','yyyy-MM-dd HH:mm:ss')"
            }
          }
          println("SRC_DBTYPE=="+SRC_DBTYPE)
          if (SRC_DBTYPE.toUpperCase.equals("ORACLE")) {
            if (MODE.equals("INC")) {
              SRC_RECON_WHERE = "where " + AUDIT_COL + " BETWEEN to_date('" + LAST_UPD_VAL_LL + "','YYYY-MM-DD HH24:MI:SS') AND to_date('" + LAST_UPD_VAL_UL + "','YYYY-MM-DD HH24:MI:SS')"
              if (TARGET_DBTYPE.toUpperCase.contains("ORACLE")) {
                TGT_RECON_WHERE = SRC_RECON_WHERE
              } else if (TARGET_DBTYPE.toUpperCase.contains("SQLSERVER")) {
                TGT_RECON_WHERE = "where " + AUDIT_COL + " BETWEEN CONVERT(DATETIME, '" + LAST_UPD_VAL_LL + "',120) AND CONVERT(DATETIME, '" + LAST_UPD_VAL_UL + "',120)"
              } else {
                TGT_RECON_WHERE = "where " + AUDIT_COL + " BETWEEN to_date('" + LAST_UPD_VAL_LL + "','YYYY-MM-DD HH24:MI:SS') AND to_date('" + LAST_UPD_VAL_UL + "','YYYY-MM-DD HH24:MI:SS')"
              }
            } else {
              SRC_RECON_WHERE = "where " + AUDIT_COL + " <= to_date('" + LAST_UPD_VAL + "','YYYY-MM-DD HH24:MI:SS')"
              if (TARGET_DBTYPE.toUpperCase.contains("ORACLE")) {
                TGT_RECON_WHERE = SRC_RECON_WHERE
              } else if (TARGET_DBTYPE.toUpperCase.contains("SQLSERVER")) {
                TGT_RECON_WHERE = "where " + AUDIT_COL + " <= CONVERT(DATETIME, '" + LAST_UPD_VAL + "',120)"
                println("TGT_RECON_WHERE1=="+TGT_RECON_WHERE)
              } else {
                TGT_RECON_WHERE = "where " + AUDIT_COL + " <= to_date('" + LAST_UPD_VAL + "','YYYY-MM-DD HH24:MI:SS')"
              }
            }
          } else if (SRC_DBTYPE.toUpperCase.equals("SQLSERVER")) {
            SRC_RECON_WHERE = "where " + AUDIT_COL + " <= CONVERT(DATETIME, '" + LAST_UPD_VAL + "',120)"
            TGT_RECON_WHERE = "where " + AUDIT_COL + " <= CONVERT(DATETIME, '" + LAST_UPD_VAL + "',120)"
            println("TGT_RECON_WHERE2=="+TGT_RECON_WHERE)
          } else if (SRC_DBTYPE.toUpperCase.equals("MYSQL")) {
            SRC_RECON_WHERE = "where " + AUDIT_COL + " <= '" + LAST_UPD_VAL + "'"
            TGT_RECON_WHERE = "where " + AUDIT_COL + " <= '" + LAST_UPD_VAL + "'"
          } else if (SRC_DBTYPE.toUpperCase.equals("AS400DB2")) {
            SRC_RECON_WHERE = "where " + AUDIT_COL + " <= '" + LAST_UPD_VAL + "'"
            TGT_RECON_WHERE = "where " + AUDIT_COL + " <= '" + LAST_UPD_VAL + "'"
          } else if (SRC_DBTYPE.toUpperCase.equals("TERADATA")) {
            SRC_RECON_WHERE = "where CAST(" + AUDIT_COL + " AS Date) <= '" + LAST_UPD_VAL + "' (Timestamp(0), Format 'YYYY-MM-DDbhh:mi:Ss')"
            TGT_RECON_WHERE = "where CAST(" + AUDIT_COL + " AS Date) <= '" + LAST_UPD_VAL + "' (Timestamp(0), Format 'YYYY-MM-DDbhh:mi:Ss')"
          }

          if (AUDIT_COL == "" && AUDIT_COL == null) {
            SRC_RECON_WHERE = ""
            TGT_RECON_WHERE = ""
          }

          //-------Source Union Query-------------------
          if (j == 0) {
            src_count_query = "Select CONCAT('" + f + "=',count(*)) as COUNT from " + SCHEMA_NAME + "." + f + " " + SRC_RECON_WHERE
          } else {
            src_count_query = src_count_query + " union all Select CONCAT('" + f + "=',count(*)) as COUNT from " + SCHEMA_NAME + "." + f + " " + SRC_RECON_WHERE
          }
          j = j + 1

          target_tbl = f
          logger.info("Target table name is " + target_tbl)
          targetData = new Properties
          fis_tgt = new FileInputStream(Target_tbl_list)
          targetData.load(fis_tgt)
          if (targetData == null) {
            logger.info("Props is null - Target_tbl_list")
            return false
          }
          target_tbl = targetData.getProperty(f)
          logger.info("Target table name is " + target_tbl)

          //-------Hive Union Query---------------------
          if (TARGET_DBTYPE.toUpperCase.equals("ORACLE") || TARGET_DBTYPE.toUpperCase.equals("SQLSERVER")) {
            if (i == 0) {
              tgt_count_query = "Select CONCAT('$target_tbl=',count(*)) as COUNT from ${SCHEMA_NAME}.$target_tbl ${TGT_RECON_WHERE}"
            } else {
              tgt_count_query = "${src_count_query} union all Select CONCAT('$target_tbl=',count(*)) as COUNT from ${SCHEMA_NAME}.$target_tbl ${TGT_RECON_WHERE}"
            }
          } else {
            if (i == 0) {
              if (DB_CHECK_ATT.equals("N")) {
                tgt_count_query = "Select count(*) as COUNT from ${DB_NAME}_RAW.$target_tbl ${HIVE_WHERE}"
              } else {
                tgt_count_query = "Select count(*) as COUNT from ${DB_NAME}_RAW_ATT.$target_tbl ${HIVE_WHERE}"
              }
            } else {
              if (DB_CHECK_ATT.equals("N")) {
                tgt_count_query = "${tgt_count_query} union all Select count(*) as COUNT from ${DB_NAME}_RAW.$target_tbl ${HIVE_WHERE}"
              } else {
                tgt_count_query = "${tgt_count_query} union all Select count(*) as COUNT from ${DB_NAME}_RAW_ATT.$target_tbl ${HIVE_WHERE}"
              }
            }
          }
          i = i + 1
        })
      }

      val FINAL1 = new File(Source_query)
      val bw1 = new BufferedWriter(new FileWriter(FINAL1))
      logger.info("Writing Source query to " + Source_query)
      src_count_query.foreach(p => bw1.write(p + "\n"))
      bw1.close()
      logger.info("File created - " + Source_query)

      Source_cnt_temp = getRDBMSCount(src_count_query, CONN_STR, USERNAME, PWD, SRC_DBTYPE).toString
      val FINAL = new File(Source_cnt_order)
      val bw = new BufferedWriter(new FileWriter(FINAL))
      logger.info("Writing to " + Source_cnt_order)
      Source_cnt_temp.foreach(p => bw.write(p + "\n"))
      bw.close()
      logger.info("File created - " + Source_cnt_order)
      FIN_TARGET = TARGET_DBTYPE

      rddFromFile.collect().foreach(f => {
        logger.info("Table name is - " + f)
        props = new Properties
        fis = new FileInputStream(Audit_list)
        props.load(fis)
        if (props == null) {
          logger.info("Props is null - Audit_list")
          return false
        }
        AUDIT_COL = props.getProperty(f)
        logger.info("Audit column is " + AUDIT_COL)

        if (MODE.equals("INC") && (TARGET_DBTYPE.toUpperCase.contains("ORACLE") || TARGET_DBTYPE.toUpperCase.contains("SQLSERVER") || TARGET_DBTYPE.toUpperCase.contains("AZURE_SQLDB"))) {
          LAST_UPD_VAL_LL = LOWER_LIMIT + " 00:00:00"
          LAST_UPD_VAL_UL = UPPER_LIMIT + " 00:00:00"
        }

        if (AUDIT_COL != "" && AUDIT_COL != null) {
          if (ENABLE_DSN != "" && ENABLE_DSN != null && ENABLE_DSN.equals("Y")) {
            DSN_WHERE = " AND DSN='" + DSN_CONDITION + "'"
          }
        } else {
          if (ENABLE_DSN != "" && ENABLE_DSN != null && ENABLE_DSN.equals("Y")) {
            DSN_WHERE = "where DSN='" + DSN_CONDITION + "'"
          }
        }

        if (SRC_DBTYPE.toUpperCase.equals("ORACLE")) {
          if (MODE.equals("INC")) {
            SRC_RECON_WHERE = "where " + AUDIT_COL + " BETWEEN to_date('" + LAST_UPD_VAL_LL + "','YYYY-MM-DD HH24:MI:SS') AND to_date('" + LAST_UPD_VAL_UL + "','YYYY-MM-DD HH24:MI:SS')"
            if (TARGET_DBTYPE.toUpperCase.contains("ORACLE")) {
              TGT_RECON_WHERE = SRC_RECON_WHERE
            } else if (TARGET_DBTYPE.toUpperCase.contains("SQLSERVER")) {
              TGT_RECON_WHERE = "where " + AUDIT_COL + " BETWEEN CONVERT(DATETIME, '" + LAST_UPD_VAL_LL + "',120) AND CONVERT(DATETIME, '" + LAST_UPD_VAL_UL + "',120)"
            } else {
              TGT_RECON_WHERE = "where " + AUDIT_COL + " BETWEEN to_date('" + LAST_UPD_VAL_LL + "','YYYY-MM-DD HH24:MI:SS') AND to_date('" + LAST_UPD_VAL_UL + "','YYYY-MM-DD HH24:MI:SS')"
            }
          } else {
            SRC_RECON_WHERE = "where " + AUDIT_COL + " <= to_date('" + LAST_UPD_VAL + "','YYYY-MM-DD HH24:MI:SS')"
            if (TARGET_DBTYPE.toUpperCase.contains("ORACLE")) {
              TGT_RECON_WHERE = SRC_RECON_WHERE
            } else if (TARGET_DBTYPE.toUpperCase.contains("SQLSERVER")) {
              TGT_RECON_WHERE = "where " + AUDIT_COL + " <= CONVERT(DATETIME, '" + LAST_UPD_VAL + "',120)"
            } else {
              TGT_RECON_WHERE = "where " + AUDIT_COL + " <= to_date('" + LAST_UPD_VAL + "','YYYY-MM-DD HH24:MI:SS')"
            }
          }
        } else if (SRC_DBTYPE.toUpperCase.equals("SQLSERVER")) {
          SRC_RECON_WHERE = "where " + AUDIT_COL + " <= CONVERT(DATETIME, '" + LAST_UPD_VAL + "',120)"
          TGT_RECON_WHERE = "where " + AUDIT_COL + " <= CONVERT(DATETIME, '" + LAST_UPD_VAL + "',120)"
        } else if (SRC_DBTYPE.toUpperCase.equals("MYSQL")) {
          SRC_RECON_WHERE = "where " + AUDIT_COL + " <= '" + LAST_UPD_VAL + "'"
          TGT_RECON_WHERE = "where " + AUDIT_COL + " <= '" + LAST_UPD_VAL + "'"
        } else if (SRC_DBTYPE.toUpperCase.equals("AS400DB2")) {
          SRC_RECON_WHERE = "where " + AUDIT_COL + " <= '" + LAST_UPD_VAL + "'"
          TGT_RECON_WHERE = "where " + AUDIT_COL + " <= '" + LAST_UPD_VAL + "'"
        } else if (SRC_DBTYPE.toUpperCase.equals("TERADATA")) {
          SRC_RECON_WHERE = "where CAST(" + AUDIT_COL + " AS Date) <= '" + LAST_UPD_VAL + "' (Timestamp(0), Format 'YYYY-MM-DDbhh:mi:Ss')"
          TGT_RECON_WHERE = "where CAST(" + AUDIT_COL + " AS Date) <= '" + LAST_UPD_VAL + "' (Timestamp(0), Format 'YYYY-MM-DDbhh:mi:Ss')"
        }

        if (AUDIT_COL == "" && AUDIT_COL == null) {
          SRC_RECON_WHERE = ""
          TGT_RECON_WHERE = ""
        }

        target_tbl = f
        /*targetData = new Properties
        fis_tgt = new FileInputStream(Target_tbl_list)
        targetData.load(fis_tgt)
        if(targetData == null) {
          logger.info("Props is null - Target_tbl_list")
          return false
        }
        target_tbl = targetData.getProperty(f)*/
        logger.info("Target table name is ===== " + target_tbl)

        if (TARGET_DBTYPE.toUpperCase.contains("ORACLE") && !TARGET_DBTYPE.toUpperCase.equals("ORACLE")) {

          fintargetData = new Properties
          fis_tgt_final = new FileInputStream(Tables_list_tgtwise)
          fintargetData.load(fis_tgt_final)
          if (fintargetData == null) {
            logger.info("Props is null - Tables_list_tgtwise")
            return false
          }
          FIN_TARGET = fintargetData.getProperty(f)
          logger.info("Final Target is " + FIN_TARGET)

        } else if (TARGET_DBTYPE.toUpperCase.contains("SQLSERVER") && !TARGET_DBTYPE.toUpperCase.equals("SQLSERVER")) {

          fintargetData = new Properties
          fis_tgt_final = new FileInputStream(Tables_list_tgtwise)
          fintargetData.load(fis_tgt_final)
          if (fintargetData == null) {
            logger.info("Props is null - Tables_list_tgtwise")
            return false
          }
          FIN_TARGET = fintargetData.getProperty(f)
          logger.info("Final Target is FIN_TARGET= " + FIN_TARGET)
        }

        if (FIN_TARGET.toUpperCase.equals("ORACLE") || FIN_TARGET.toUpperCase.equals("SQLSERVER")) {
          if (TGT_TBLNM_PREFIX != "" && TGT_TBLNM_PREFIX != null) {
            target_tbl = TGT_TBLNM_PREFIX + target_tbl
          } else if (TGT_TBLNM_SUFFIX != "" && TGT_TBLNM_SUFFIX != null) {
            target_tbl = target_tbl + TGT_TBLNM_SUFFIX
          }





          tgt_count_query = "Select count(*) as COUNT from "+TGT_SCHEMA_NAME+"."+target_tbl+" "+TGT_RECON_WHERE+" "+DSN_WHERE+" "+DEL_WHERE

          // tgt_count_query = "Select count(*) as COUNT from DBO.MPI_ALERT where CREATEDON <= 2021-05-08 "


          //tgt_count_query = "Select count(*) as COUNT from ${TGT_SCHEMA_NAME}.$target_tbl ${TGT_RECON_WHERE}${DSN_WHERE} ${DEL_WHERE}"



          println("tgt_count_query==="+tgt_count_query)
          //logger.info("tgt_count_query " + f + " is : " + tgt_count_query)

          lines ++= tgt_count_query + "\n"
          Target_cnt = getRDBMSCount(tgt_count_query, TGT_CONN_STR, TGT_USERNAME, TGT_PWD, TARGET_DBTYPE)
        } else {
          if (DB_CHECK_ATT.equals("N")) {
            tgt_count_query = "Select count(*) as COUNT from " + HIVE_DBNAME + "_RAW." + target_tbl + HIVE_WHERE
            val tgt_cnt = spark.sql(tgt_count_query)
            if (tgt_cnt != null) {
              val rows = tgt_cnt.select("*").collect()
              if (rows != null && rows.length > 0) {
                if (rows(0) != null && rows(0)(0) != null) {
                  Target_cnt = rows(0)(0).toString
                }
              }
            } else {
              tgt_count_query = "Select count(*) as COUNT from " + HIVE_DBNAME + "_RAW_ATT." + target_tbl + HIVE_WHERE
              val tgt_cnt = spark.sql(tgt_count_query)
              if (tgt_cnt != null) {
                val rows = tgt_cnt.select("*").collect()
                if (rows != null && rows.length > 0) {
                  if (rows(0) != null && rows(0)(0) != null) {
                    Target_cnt = rows(0)(0).toString
                  }
                }
              }
            }
          }
        }
        println("Source_cnt_order=="+Source_cnt_order)

        srccountData = new Properties
        fis_srccnt = new FileInputStream(Source_cnt_order)
        srccountData.load(fis_srccnt)
        if (srccountData == null) {
          logger.info("Props is null - Source_cnt_order")
          return false
        }

        println("Source_cnt_order==f="+f)
        Source_cnt = srccountData.getProperty(f)

        if (Diff_cnt == 0) {
          Recon_status = "PASSED"
          logger.info("Src_count for " + f + ": " + Source_cnt + " and Hive_count for " + f + ": " + Target_cnt + " - Reconciliation PASSED")
        } else {
          Recon_status = "FAILED"
          logger.info("Src_count for " + f + ": " + Source_cnt + " and Hive_count for " + f + ": " + Target_cnt + " - " + Diff_cnt + " - Reconciliation FAILED")
        }
        logger.info(Recon_status)

        UniqueID = "Attunity_" + PROJ_NAME + "_" + f + "_" + format_temp.format(START_TIME.getTime)
        UPD_VAL = LAST_UPD_VAL

        if (MODE.equals("INC") && (TARGET_DBTYPE.toUpperCase.contains("ORACLE") || TARGET_DBTYPE.toUpperCase.contains("SQLSERVER"))) {
          UPD_VAL = LAST_UPD_VAL_LL
        }

        if (TARGET_DBTYPE.toUpperCase.equals("ORACLE") || TARGET_DBTYPE.toUpperCase.equals("SQLSERVER")) {
          if (!insertReconDetails(f, TGT_SCHEMA_NAME, Source_cnt, Target_cnt, Diff_cnt, UPD_VAL)) {
            logger.error("Error When inserting status to ReconDetails table")
            //sys.exit(1)
            return false
          }
        } else {
          if (!insertReconDetails(f, HIVE_DBNAME, Source_cnt, Target_cnt, Diff_cnt, LAST_UPD_VAL)) {
            logger.error("Error When inserting status to ReconDetails table")
            //sys.exit(1)
            return false
          }
        }
      })

      val FINAL2 = new File(Target_query)
      val bw2 = new BufferedWriter(new FileWriter(FINAL2))
      logger.info("Writing Target query to " + Target_query)
      lines.foreach(p => bw2.write(p + "\n"))
      bw2.close()
      logger.info("File created - " + Target_query)

      if (!updateMaxDate()) {
        logger.error("ERROR updating Control table..")
      }

      // Setup mail server
      properties.put("mail.smtp.host", host)
      properties.put("mail.smtp.port", port)
      val session = Session.getInstance(properties)

      // Create a default MimeMessage object
      val msg = new MimeMessage(session)
      msg.setFrom(new InternetAddress(from))
      msg.setRecipients(Message.RecipientType.TO, to);

      msg.setSubject("ATTUNITY_RECON")
      msg.setText("RECON for " + PROJ_NAME + " is completed..")
      Transport.send(msg)

      logger.info("Logger is closing")
      LogManager.shutdown();
      logger.info("Logger closed")
    } catch {
      case e: FileNotFoundException => e.printStackTrace
    } finally {
      logger.info("Logger is closing")
      LogManager.shutdown();
      logger.info("Logger closed")
    }
  }

  def InitializeObjects(args: Array[String]): Boolean = {

    props = new Properties
    log_props = new Properties()

    try {

      PROJ_NAME = args(0)
      logger.info("Project name is " + PROJ_NAME)

      logger.info("Reading the file prop.txt..")
      fis = new FileInputStream("/dbfs" + base_path + "prop.txt")
      props.load(fis)
      master = props.getProperty("master")
      ADLS_URL = props.getProperty("ADLS_Path")
      AUDIT_USERNAME = props.getProperty("AUDIT_USERNAME")
      AUDIT_PASSWD = props.getProperty("AUDIT_PASSWD")
      AUDIT_CONN_STRING = props.getProperty("AUDIT_CONN_STRING")
      AUDIT_DBTYPE = props.getProperty("AUDIT_DBTYPE")
      AUDIT_HOSTNAME = props.getProperty("AUDIT_HOSTNAME")
      AUDIT_CONN_DBNAME = props.getProperty("AUDIT_CONN_DBNAME")
      AUDIT_RECON_TBL = "elf.data_reconciliation_details"
      AUDIT_CNTRL_TBL = "elf.attunity_contol_table"

      DL_path = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME
      Source_Conn_Path = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/" + PROJ_NAME + "_CONNECTION.txt"
      Target_Conn_Path = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/" + PROJ_NAME + "_TARGET_CONNECTION.txt"
      Source_query = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/Source_Query.txt"
      Target_query = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/Target_Query.txt"
      Source_cnt = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/Source_Count.txt"
      Source_cnt_order = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/Source_Count_Order.txt"
      Target_cnt = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/Hive_Count.txt"
      Temp_file = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/temp.txt"
      Tables_list = "dbfs:" + base_path + "attunity_recon/" + PROJ_NAME + "/Tables_List.txt"
      Tables_list_tgtwise = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/Tables_List_Tgtwise.txt"
      Audit_list = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/Audit_List.txt"
      Target_tbl_list = "/dbfs" + base_path + "attunity_recon/" + PROJ_NAME + "/Target_Table_Name_List.txt"

      if (args(1) != null && args(1) != "" && args(1).equals("INC")) {
        MODE = args(1)
        LOWER_LIMIT = args(2)
        UPPER_LIMIT = args(3)
        //LOWER_LIMIT=2020-03-06
        //UPPER_LIMIT=2020-03-07

      } else if (args(1) != "" && args(1) != null && args(1).equals("-f")) {
        FREQ = args(2).toInt
      }

      format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
      format_temp = new SimpleDateFormat("YYYYMMddHHmmss")
      START_TIME = Calendar.getInstance

      val now = Calendar.getInstance().getTime()
      Job_Start_datetime = format.format(now)

      JOB_ID = "ATTUNITY_RECON_" + PROJ_NAME + "_" + format_temp.format(START_TIME.getTime)
      LOG_ROOT_DIR = "/dbfs" + base_path + "attunity_recon/logs/"
      fis_logs = new FileInputStream("/dbfs" + base_path + "log4j.properties")
      log_props.load(fis_logs)
      return true
    } catch {
      case e: Exception => {
        logger.error(e.toString)
        throw new RuntimeException(e)
        return false
      }
    } finally {
      if (null != fis) {
        try {
          fis.close();
          logger.info("File Stream used for Property file is closed")
        } catch {
          case e: Exception => {
            logger.error(e.toString)
            logger.error("Error closing the File input stream")
            throw new RuntimeException(e)
          }
        }
      }
      if (null != fis_logs) fis_logs.close()
    }
    return true
  }

  def initializeLog4j(): Boolean = {

    log_props.remove("log4j.appender.FILE.File")
    log_props.setProperty("log4j.appender.FILE.File", LOG_ROOT_DIR + JOB_ID + ".log")
    LogManager.resetConfiguration()
    PropertyConfigurator.configure(log_props)
    return true
  }

  def readConnectionFile(): Boolean = {

    propertyData = new Properties
    source_conn_file = new FileInputStream(Source_Conn_Path)
    propertyData.load(source_conn_file)
    if (propertyData == null) {
      logger.info("Props is null - Source_Conn_Path")
      return false
    }

    CONN_STR = propertyData.getProperty("CONNECTION_STRING")
    logger.info("Connection String used is " + CONN_STR)
    USERNAME = propertyData.getProperty("USERNAME")
    logger.info("User Name is " + USERNAME)
    SRC_DBTYPE = propertyData.getProperty("DB_TYPE")
    logger.info("Source DB Type is " + SRC_DBTYPE)
    INC_MODE = propertyData.getProperty("AUDIT_COL_TYPE")
    logger.info("Increment Mode is " + INC_MODE)
    PWD = propertyData.getProperty("PASSWORD")
    logger.info("Retrieved Password")
    SCHEMA_NAME = propertyData.getProperty("SCHEMA_NAME")
    logger.info("Schema Name is " + SCHEMA_NAME)
    if (propertyData.getProperty("DB_NAME") != null && propertyData.getProperty("DB_NAME").length > 0) {
      HIVE_DBNAME = propertyData.getProperty("DB_NAME")
      logger.info("Hive DB name is " + HIVE_DBNAME)
    }
    TARGET_DBTYPE = propertyData.getProperty("TARGET")
    logger.info("TARGET DB Type is " + TARGET_DBTYPE)
    if (propertyData.getProperty("DB_NAME_ATT") != null && propertyData.getProperty("DB_NAME_ATT").length > 0) {
      DB_NAME_ATT = propertyData.getProperty("DB_NAME_ATT")
      logger.info("DB Name Check used is " + DB_NAME_ATT)
      if (DB_NAME_ATT != "" || DB_NAME_ATT != null) {
        DB_CHECK_ATT = "Y"
      }
    }

    return true
  }

  def readTargetConnectionFile(): Boolean = {

    propertyData = new Properties
    target_conn_file = new FileInputStream(Target_Conn_Path)
    propertyData.load(target_conn_file)
    if (propertyData == null) {
      logger.info("Props is null - Target_Conn_Path")
      return false
    }

    TGT_CONN_STR = propertyData.getProperty("CONNECTION_STRING")
    logger.info("Connection String used is " + TGT_CONN_STR)
    TGT_USERNAME = propertyData.getProperty("USERNAME")
    logger.info("User Name is " + TGT_USERNAME)
    TGT_PWD = propertyData.getProperty("PASSWORD")
    logger.info("Retrieved Password")
    TGT_SCHEMA_NAME = propertyData.getProperty("SCHEMA_NAME")
    logger.info("Schema Name is " + TGT_SCHEMA_NAME)
    if (propertyData.getProperty("TBL_NM_PREFIX") != null && propertyData.getProperty("TBL_NM_PREFIX").length > 0) {
      TGT_TBLNM_PREFIX = propertyData.getProperty("TBL_NM_PREFIX")
      logger.info("Table name Prefix is " + TGT_TBLNM_PREFIX)
    }
    if (propertyData.getProperty("TBL_NM_SUFFIX") != null && propertyData.getProperty("TBL_NM_SUFFIX").length > 0) {
      TGT_TBLNM_SUFFIX = propertyData.getProperty("TBL_NM_SUFFIX")
      logger.info("Table name Suffix is " + TGT_TBLNM_SUFFIX)
    }
    if (propertyData.getProperty("DELETE_COLUMN_NAME") != null && propertyData.getProperty("DELETE_COLUMN_NAME").length > 0) {
      DEL_COL = propertyData.getProperty("DELETE_COLUMN_NAME")
      logger.info("Hive DB name is " + DEL_COL)
      if (DEL_COL != "" && DEL_COL != null) {
        DEL_WHERE = "AND " + DEL_COL + " NOT in ('D')"
        logger.info("Delete condition is " + DEL_WHERE)
      }
    }
    if (propertyData.getProperty("ENABLE_DSN") != null && propertyData.getProperty("ENABLE_DSN").length > 0) {
      ENABLE_DSN = propertyData.getProperty("ENABLE_DSN")
      logger.info("DSN Flag is " + ENABLE_DSN)
    }
    if (ENABLE_DSN != "" && ENABLE_DSN != null && ENABLE_DSN.equals("Y")) {
      DSN_CONDITION = propertyData.getProperty("DSN_CONDITION")
      logger.info("DSN condition is " + DSN_CONDITION)
    }
    return true
  }

  def return_property(filepath: String): Properties = {

    try {
      fs = FileSystem.get(URI.create(ADLS_URL), config)
      DSInputStream = fs.open(new org.apache.hadoop.fs.Path(filepath))
      Prop.load(DSInputStream)
      DSInputStream.close()
      fs.close()
    } catch {
      case e: Exception => {
        return null
      }
    } finally {
      if (InputStream != null) {
        InputStream.close()
        //return false
      }
      if (DSInputStream != null) DSInputStream.close()
      if (fs != null) fs.close()
    }
    return Prop
  }

  def GetMaxDate(): Boolean = {

    try {

      //val query = "(select concat(TASK_NAME,'|',LAST_UPD_VALUE,'|',RECON_FREQUENCY_IN_MINS) as DF from " + AUDIT_CNTRL_TBL + " where TASK_NAME='"+PROJ_NAME+"') foo"
      val query = "(select LAST_UPD_VALUE from " + AUDIT_CNTRL_TBL + " where TASK_NAME='" + PROJ_NAME + "') foo"

      logger.info("Read data from metastore..")
      logger.info("Query - " + query)
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      val DF = spark.sqlContext.read.format("jdbc").
        option("url", AUDIT_CONN_STRING).
        option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
        option("user", AUDIT_USERNAME).option("password", AUDIT_PASSWD).option("dbtable", query).load()

      val rows = DF.select("*").collect()
      if (rows(0) != null) {
        LAST_UPD_VAL = rows(0).toString()

      }

      /* if(SRC_DBTYPE.toUpperCase.equals("SQLSERVER")){
         println("LAST_UPD_VAL===SQLSERVER")
           LAST_UPD_VAL= LAST_UPD_VAL.substring(1, 19)
           println("LAST_UPD_VAL===SQLSERVER=="+LAST_UPD_VAL)
       }*/

      if (SRC_DBTYPE.toUpperCase.equals("ORACLE")) {
        LAST_UPD_VAL = LAST_UPD_VAL.substring(1, 19)
        println("LAST_UPD_VAL===ORACLE=="+LAST_UPD_VAL)
        last_upd_val_query = "select cast('" + LAST_UPD_VAL + "' as TIMESTAMP) + INTERVAL " + FREQ + " minutes"
      } else if (SRC_DBTYPE.toUpperCase.equals("SQLSERVER") || SRC_DBTYPE.toUpperCase.equals("MYSQL")) {
        last_upd_val_query = "select cast('" + LAST_UPD_VAL + "' as TIMESTAMP) + INTERVAL " + FREQ + " minutes"
      } else if (SRC_DBTYPE.toUpperCase.equals("AS400DB2")) {
        last_upd_val_query = "select cast('" + LAST_UPD_VAL + "' as TIMESTAMP) + INTERVAL 1 day"
      }

      val max = spark.sql(last_upd_val_query)
      if (max != null) {
        val rows = max.select("*").collect()
        if (rows != null && rows.length > 0) {
          if (rows(0) != null && rows(0)(0) != null) {
            LAST_UPD_VAL_UPDATED = rows(0)(0).toString()
          }
        }
      }

      return true
    } catch {
      case e: SQLException =>
        //e.printStackTrace()
        logger.error(e.getMessage)
        //System.exit(1)
        return false
    }
  }

  def getRDBMSCount(query: String, CONN_STR: String, USERNAME: String, PWD: String, DBTYPE: String): String = {

    try {
      import java.sql.DriverManager
      if (DBTYPE.toUpperCase.equals("ORACLE")) {
        driver1 = "oracle.jdbc.driver.OracleDriver"
      } else if (DBTYPE.toUpperCase.equals("AS400DB2")) {
        driver1 = "com.ibm.as400.access.AS400JDBCDriver"
      } else if (DBTYPE.toUpperCase.equals("SQLSERVER")) {
        driver1 = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      } else if (DBTYPE.toUpperCase.equals("MYSQL")) {
        driver1 = "com.mysql.jdbc.Driver"
      } else if (DBTYPE.toUpperCase.equals("TERADATA")) {
        driver1 = "com.teradata.jdbc.TeraDriver"
      }
      logger.info("getRDBMSCount CONN_STR driver1== " + driver1)
      Class.forName(driver1)
      logger.info("getRDBMSCount CONN_STR== " + CONN_STR)
      logger.info("getRDBMSCount USERNAME== " + USERNAME)
      logger.info("getRDBMSCount== DBTYPE== " + DBTYPE)
      logger.info("getRDBMSCount CONN_STR PWD== " + PWD)

      val str="s"
      if(DBTYPE.toUpperCase.equals("SQLSERVER")) {
        //jdbc:sqlserver://172.28.170.18:1433;database=FlexNet
        val t=str+CONN_STR
        println("str+CONN_STR="+t)
        conn = DriverManager.getConnection(str+CONN_STR, USERNAME, PWD)
        //conn = DriverManager.getConnection(s"jdbc:sqlserver://172.28.170.18:1433;database=FlexNet", USERNAME, PWD)
        println("Connected====")
      }else

        conn = DriverManager.getConnection(CONN_STR, USERNAME, PWD)

      if (DBTYPE.toUpperCase.equals("TERADATA"))
        conn.setTransactionIsolation(1)
      else if (!DBTYPE.toUpperCase.equals("ORACLE"))
        conn.setTransactionIsolation(0)
      logger.info("getRDBMSCount==connecected")
      stmnt = conn.createStatement
      logger.info("Executing Query " + query)

      res = stmnt.executeQuery(query)
      var count: String = ""
      while (res.next) {
        count = res.getString(1)
      }
      logger.info("Count is " + count)
      res.close
      stmnt.close
      conn.close()
      return count
    }
    catch {
      case e: SQLException =>
        //e.printStackTrace()
        logger.error(e.getMessage)
        logger.error("Error Getting RDBMS count")
        //System.exit(1)
        return ""
    } finally {
      if (stmnt != null) stmnt.close()
      if (conn != null) conn.close()
    }
  }

  def insertReconDetails(tbl: String, TARGET_DB: String, source_cnt: String, target_cnt: String, diff_cnt: Int, last_upd: String): Boolean = {

    try {
      conn = DriverManager.getConnection(s"jdbc:sqlserver://" + AUDIT_HOSTNAME + ":" + AUDITPORT + ";database=" + AUDIT_CONN_DBNAME, AUDIT_USERNAME, AUDIT_PASSWD)
      insertquery = "Insert into " + AUDIT_RECON_TBL + " values ('" + UniqueID + "','" + UniqueID + "','" + HIVE_DBNAME + "','" + SCHEMA_NAME + "','" + tbl + "','" + TARGET_DB + "','" + target_tbl + "','" + Job_Start_datetime + "','" + Recon_Start_datetime + "','" + "COUNT" + "','" + source_cnt + "','" + target_cnt + "','" + diff_cnt + "','" + Recon_Start_datetime + "','" + "NULL" + "','" + "part-files" + "','" + 0 + "','" + source_cnt + "','" + target_cnt + "','" + "SUCCESS" + "','" + AUDIT_COL + "','" + INC_MODE + "','" + last_upd + "','" + Recon_status + "')"

      logger.info(insertquery)
      conn.setAutoCommit(true)
      stmnt = conn.createStatement
      val insres = stmnt.executeUpdate(insertquery)

      stmnt.close
      conn.close()

      if (insres == 1)
        return true
      else
        return false
    } catch {
      case e: SQLException =>
        //e.printStackTrace()
        logger.error(e.getMessage)
        //System.exit(1)
        return false
        throw new RuntimeException(e)
    } finally {
      if (stmnt != null) stmnt.close()
      if (conn != null) conn.close()
    }
  }

  def updateMaxDate(): Boolean = {

    try {
      conn = DriverManager.getConnection(s"jdbc:sqlserver://" + AUDIT_HOSTNAME + ":" + AUDITPORT + ";database=" + AUDIT_CONN_DBNAME, AUDIT_USERNAME, AUDIT_PASSWD)
      val updatequery = "UPDATE " + AUDIT_CNTRL_TBL + " SET RECON_DATETIME= '" + Recon_Start_datetime + "', LAST_UPD_VALUE= '" + LAST_UPD_VAL_UPDATED + "', SOURCE_DB_TYPE= '" + SRC_DBTYPE + "', AUDIT_COL_TYPE= '" + INC_MODE + "' WHERE TASK_NAME='" + PROJ_NAME + "'"

      logger.info(updatequery)
      conn.setAutoCommit(true)
      stmnt = conn.createStatement
      val insres = stmnt.executeUpdate(updatequery)

      stmnt.close
      conn.close()

      if (insres == 1)
        return true
      else
        return false
    } catch {
      case e: SQLException =>
        //e.printStackTrace()
        logger.error(e.getMessage)
        //System.exit(1)
        return false
        throw new RuntimeException(e)
    } finally {
      if (stmnt != null) stmnt.close()
      if (conn != null) conn.close()
    }
  }

}

