package codes.recursive;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CompatibleConsumer {

    public void consume() {
        String authToken = System.getenv("AUTH_TOKEN");
        String tenancyName = System.getenv("TENANCY_NAME");
        String username = System.getenv("STREAMING_USERNAME");
        String compartmentId = System.getenv("COMPARTMENT_ID");
        String topicName = System.getenv("TOPIC_NAME");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "streaming.us-phoenix-1.oci.oraclecloud.com:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-0");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + tenancyName + "/"
                        + username + "/"
                        + compartmentId + "\" "
                        + "password=\""
                        + authToken + "\";"
        );
        properties.put("max.partition.fetch.bytes", 1024 * 1024); // limit request size to 1MB per partition

        Consumer<Long, String> consumer = new KafkaConsumer<>(properties);

        try {
            consumer.subscribe(Collections.singletonList( topicName ) );

            while(true) {
                Duration duration = Duration.ofMillis(1000);
                ConsumerRecords<Long, String> consumerRecords = consumer.poll(duration);
                consumerRecords.forEach(record -> {
                    System.out.println("Record Key " + record.key());
                    System.out.println("Record value " + record.value());
                    System.out.println("Record partition " + record.partition());
                    System.out.println("Record offset " + record.offset());
                });
                // commits the offset of record to broker.
                consumer.commitAsync();
            }
        }
        catch(WakeupException e) {
            // do nothing, shutting down...
        }
        finally {
            System.out.println("closing consumer");
            consumer.close();
        }

    }

}
