package OrinToHBase;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;  


public class MyProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "172.31.17.200:9092,172.31.17.26:9092,172.31.17.209:9092");
        kafkaProperties.put("zookeeper.connect", "172.31.17.200:2181,172.31.17.26:2181,172.31.17.29:2181");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(kafkaProperties);
        ProducerRecord record = new ProducerRecord("test", "name", "water");
        try {
            Future result = producer.send(record);
            System.out.println(result.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
