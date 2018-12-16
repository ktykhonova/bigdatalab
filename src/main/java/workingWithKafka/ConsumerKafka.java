package workingWithKafka;//output

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class ConsumerKafka {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Consumer
        KafkaConsumer consumerKafka1 = new KafkaConsumer(properties);
        KafkaConsumer consumerKafka2 = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("testSeveralReplications");
        consumerKafka1.subscribe(topics);
        consumerKafka2.subscribe(topics);
        ArrayList<String> kafkaMessages = new ArrayList<String>(10000);
        int counter1 =0;
        int counter2 =0;
        try{
            while (true){
                ConsumerRecords<String, String> recordsFromConsumer1 = consumerKafka1.poll(Duration.ofSeconds(1));
                for (ConsumerRecord record: recordsFromConsumer1){
                    System.out.println(record.value().toString());
                    counter1++;
                }
                ConsumerRecords<String, String> recordsFromConsumer2 = consumerKafka2.poll(Duration.ofSeconds(1));
                for (ConsumerRecord record: recordsFromConsumer2){
                    System.out.println(record.value().toString());
                    counter2++;
                }
                if(counter1+counter2==10000){
                    System.out.println("counter1: "+counter1 +"; counter2: "+counter2);
                    break;
                }

            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            consumerKafka1.close();
            consumerKafka2.close();
        }
    }

    public static ArrayList<String> getKafkaMessages(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Consumer
        KafkaConsumer consumerKafka = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("VisitsTopic");
        consumerKafka.subscribe(topics);
        ArrayList<String> kafkaMessages = new ArrayList<String>(10000);
        int counter =0;
        try{
            while (true){
                ConsumerRecords<String, String> recordsFromConsumer1 = consumerKafka.poll(Duration.ofSeconds(10));
                for (ConsumerRecord record: recordsFromConsumer1){
                    kafkaMessages.add(record.value().toString());
                    counter++;
                }
                if(counter==10000){
                    System.out.println(counter);
                    break;
                }

            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            consumerKafka.close();
        }
        return kafkaMessages;
    }
}