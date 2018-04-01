package xyz.fomichev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProduceData {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.50.2:9092,192.168.50.3:9092,192.168.50.3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.connect.json.JsonSerializer.class);

        Producer<String, JsonNode> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();
        ApplicationDTO applicationDTO = new ApplicationDTO();
        applicationDTO.setId("ID1034");
        applicationDTO.setKey("name");
        applicationDTO.setValue("Ivan");
        JsonNode jsonNode = objectMapper.valueToTree(applicationDTO);
        producer.send(new ProducerRecord<>("application-topic", jsonNode));

        producer.close();

    }

}
