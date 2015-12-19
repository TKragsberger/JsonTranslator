package com.mycompany.jsontranslator;


import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONException;
import org.json.JSONObject;
import org.xml.sax.SAXException;

/**
 *
 * @author Thomas Kragsberger
 */
public class JsonTranslator {

    /**
     * @param args the command line arguments
     */
    public static int numberOfDays(String toDate)
   {   
       String fromDate = "1970-01-01";
       java.util.Calendar cal1 = new java.util.GregorianCalendar();
       java.util.Calendar cal2 = new java.util.GregorianCalendar();

       //split year, month and days from the date using StringBuffer.
       StringBuffer sBuffer = new StringBuffer(fromDate);
       String yearFrom = sBuffer.substring(0,3);
       String monFrom = sBuffer.substring(5,6);
       String ddFrom = sBuffer.substring(8,9);
       int intYearFrom = Integer.parseInt(yearFrom);
       int intMonFrom = Integer.parseInt(monFrom);
       int intDdFrom = Integer.parseInt(ddFrom);

       // set the fromDate in java.util.Calendar
       cal1.set(intYearFrom, intMonFrom, intDdFrom);

       //split year, month and days from the date using StringBuffer.
       StringBuffer sBuffer1 = new StringBuffer(toDate);
       String yearTo = sBuffer1.substring(0,3);
       String monTo = sBuffer1.substring(5,6);
       String ddTo = sBuffer1.substring(8,9);
       int intYearTo = Integer.parseInt(yearTo);
       int intMonTo = Integer.parseInt(monTo);
       int intDdTo = Integer.parseInt(ddTo);

       // set the toDate in java.util.Calendar
       cal2.set(intYearTo, intMonTo, intDdTo);

       //call method daysBetween to get the number of days between two dates
       int days = daysBetween(cal1.getTime(),cal2.getTime());
       return days;
   }

   //This method is called by the above method numberOfDays
   public static int daysBetween(Date d1, Date d2)
   {
      return (int)( (d1.getTime() - d2.getTime()) / (1000 * 60 * 60 * 24));
   }
    
    public static ConnectionFactory getConnection() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("datdb.cphbusiness.dk");
        factory.setPort(5672);
        factory.setUsername("student");
        factory.setPassword("cph");
        return factory;
    }

    public static JSONObject getJSON(String ssn, String creditScore, String loanAmount, String loanDuration) throws ParserConfigurationException, SAXException, IOException, JSONException {
        JSONObject json = new JSONObject();
        json.put("ssn", ssn);
        json.put("creditScore", creditScore);
        json.put("loanAmount", loanAmount);
        json.put("loanDuration", numberOfDays(loanDuration));

        return json;
    }

    private final static String QUEUE_NAME_BANK = "group10.bankJSON";
    private final static String EXCHANGE_NAME = "cphbusiness.bankJSON";
    private final static String REPLY_QUEUE_NAME = "group10.replyChannel.bankJSON";
    
    public static void main(String[] args) throws IOException {

        Connection connection = getConnection().newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME_BANK, false, false, false, null);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
                JSONObject json = null;
                try {
                    String[]parts = message.split(">>>");
                    json = getJSON(parts[0], parts[3], parts[1], parts[2]);
                } catch (ParserConfigurationException ex) {
                    Logger.getLogger(JsonTranslator.class.getName()).log(Level.SEVERE, null, ex);
                } catch (SAXException ex) {
                    Logger.getLogger(JsonTranslator.class.getName()).log(Level.SEVERE, null, ex);
                } catch (JSONException ex) {
                    Logger.getLogger(JsonTranslator.class.getName()).log(Level.SEVERE, null, ex);
                }
                AMQP.BasicProperties.Builder replyChannel = new AMQP.BasicProperties.Builder();
                replyChannel.replyTo(REPLY_QUEUE_NAME);
                Connection connection2 = getConnection().newConnection();
                Channel channel2 = connection2.createChannel();
                channel2.exchangeDeclare(EXCHANGE_NAME, "fanout");

                channel2.basicPublish(EXCHANGE_NAME, "", replyChannel.build(), json.toString().getBytes());
                System.out.println(" [x] Sent '" + json.toString() + "'");
                channel2.close();
                connection2.close();
//                ArrayList arraylist = new ArrayList<>();
//                    for(byte b : body) {
//                        arraylist.add(new Byte(b));
//                    }
//                    for(Object s : arraylist){
//                        System.out.println(""+s);
//                    }
//                    Connection connection2 = getConnection().newConnection();
//                    Channel channel2 = connection2.createChannel();
//                    channel2.queueDeclare(QUEUE_NAME_CPH, false, false, false, null);
//                try {
//                    channel2.basicPublish(QUEUE_NAME_CPH, "", null, getXml(arraylist.get(0)+"", arraylist.get(1)+"", arraylist.get(2)+"", arraylist.get(3)+"").getBytes());
//                } catch (ParserConfigurationException ex) {
//                    Logger.getLogger(XmlTranslator.class.getName()).log(Level.SEVERE, null, ex);
//                } catch (SAXException ex) {
//                    Logger.getLogger(XmlTranslator.class.getName()).log(Level.SEVERE, null, ex);
//                }
//                    channel2.close();
//                    connection2.close();
            }
        };
        
        channel.basicConsume(QUEUE_NAME_BANK, true, consumer);
    }

}

