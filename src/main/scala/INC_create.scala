import java.util.{Calendar, Date, Properties, TimeZone}
import java.text.SimpleDateFormat
//import javax.mail.Message
//import javax.mail.Session
//import javax.mail.Transport
//import javax.mail.internet.InternetAddress
//import javax.mail.internet.MimeMessage
import java.sql.{ Connection, ResultSet, SQLException, Statement }
import java.sql.DriverManager

object INC_Creation{

  val LOAD_TYPE_AUDIT_TBL = "elf.attunity_table_loadtype"
  val Recon_TBL = "elf.attunity_data_reconciliation_details"
//  var msg: MimeMessage = null
//  var FROM_MAIL: String = "lu305@cummins.com"
//  var TO_MAIL: String = "sa033@cummins.com"
//  var HOST: String = "10.208.0.104"
//  var PORT: String = "25"
//  var mail_properties = new Properties()
  //from = props.getProperty("FROM_MAIL")
  //to = props.getProperty("TO_MAIL")
  //host = props.getProperty("SMTP_HOST")
  //port = props.getProperty("SMTP_PORT")
  def main(args: Array[String]) : Unit = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

  }
//  def sendMail(): Unit = {
//    mail_properties.put("mail.smtp.host", HOST)
//    mail_properties.put("mail.smtp.port", PORT)
//    val session = Session.getInstance(mail_properties)
//    msg = new MimeMessage(session)
//    msg.setFrom(new InternetAddress(FROM_MAIL))
//    msg.setRecipients(Message.RecipientType.TO, TO_MAIL);
//    msg.setSubject("This is Test Mail Subject")
//    msg.setText("This is test mail Body.")
//    Transport.send(msg)
//  }

}
