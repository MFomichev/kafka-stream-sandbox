package xyz.fomichev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wsrm");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.50.2:9092,192.168.50.3:9092,192.168.50.3:9092");

        final StreamsBuilder builder = new StreamsBuilder();

        ObjectMapper objectMapper = new ObjectMapper();
        KGroupedStream<String, JsonNode> groupByKey = builder.stream("application-topic",
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer()))
        )
                .peek(
                        (key, value) -> System.err.println(key + value)
                )
                .map(
                        (key, value) -> new KeyValue<>(value.get("id").asText(), value)
                ).groupByKey(Serialized.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())));

        KTable<String, JsonNode> kTable = groupByKey.aggregate(
                    () -> objectMapper.valueToTree(new Application()
                ),
                (key, value, aggregate) -> {
                    try {
                        ApplicationDTO dto = objectMapper.readerFor(ApplicationDTO.class).readValue(value);
                        Application application = objectMapper.readerFor(Application.class).readValue(aggregate);
                        application.setDTO(dto);
                        return objectMapper.valueToTree(application);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return aggregate;
                }, Materialized.as("application-store").with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer())));
        kTable.toStream()
                .foreach((key, value) -> System.out.println(key + ":::" + value));


        final Topology topology = builder.build();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
