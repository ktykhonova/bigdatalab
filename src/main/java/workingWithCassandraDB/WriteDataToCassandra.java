package workingWithCassandraDB;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import workingWithKafka.UserVisitEntity;
import workingWithKafka.ConsumerKafka;
import workingWithKafka.DataDeserializer;

import java.text.ParseException;
import java.util.ArrayList;

public class WriteDataToCassandra {
    private static CassandraConnector client = new CassandraConnector();
    static ArrayList<String> kafkaMessages = new ConsumerKafka().getKafkaMessages();

    public static void main(String [] args) throws ParseException {
        Session session = client.getSession();
        ArrayList<UserVisitEntity> userVisits = new DataDeserializer().deserializeData(kafkaMessages);

        int i=0;
        for(UserVisitEntity userVisit: userVisits) {
            try {
                ResultSet rs = session.execute("insert into user_visits (id, sourceIP, destURL, visitDate,adRevenue,userAgent,countryCode,languageCode,searchWord,duration) values ("
                        + i + ",'"
                        + userVisit.getSourceIP().replace("'", "") + "','"
                        + userVisit.getDestURL().replace("'", "") + "','"
                        + userVisit.getVisitDate().replace("'", "") + "',"
                        + userVisit.getAdRevenue() + ",'"
                        + userVisit.getUserAgent().replace("'", "") + "','"
                        + userVisit.getCountryCode().replace("'", "") + "','"
                        + userVisit.getLanguageCode().replace("'", "") + "','"
                        + userVisit.getSearchWord().replace("'", "") + "',"
                        + userVisit.getDuration() + ");");
                i++;
                System.out.println(i);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        System.out.println(i+" records were written to Cassandra keyspace.");
    }
}
