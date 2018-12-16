package workingWithKafka;//publishing messages to Kafka cluster
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class ProducerKafka {

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        ArrayList<String> userVisits = new GetData().getDataFromFiles();
        for(int i=0; i<userVisits.size(); i++){
            String [] userVisit = userVisits.get(i).split(",");
            try {
                    kafkaProducer.send(new ProducerRecord( "testSeveralReplications",i+"",
                            "sourceIP:" + userVisit[0] +
                            ",destURL:" + userVisit[1] +
                            ",visitDate:" + userVisit[2] +
                            ",adRevenue:" + userVisit[3] +
                            ",userAgent:" + userVisit[4] +
                            ",countryCode:" + userVisit[5] +
                            ",languageCode:" + userVisit[6] +
                            ",searchWord:" + userVisit[7] +
                            ",duration:" + userVisit[8]));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        kafkaProducer.close();


    }
}

