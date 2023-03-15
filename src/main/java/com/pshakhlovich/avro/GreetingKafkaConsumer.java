package com.pshakhlovich.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

public class GreetingKafkaConsumer {
    private final String topic;
    private final KafkaConsumer<String, Greeting> consumer;


    public GreetingKafkaConsumer(String bootstrapServers, String topic, String schemaRegistry) {
        this.topic = topic;
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);

        consumer = new KafkaConsumer<>(props);
    }

    public void consume() {
        consumer.subscribe(List.of(topic));
        try {
            while(true) {
                ConsumerRecords<String, Greeting> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Greeting> record: records) {
                    Greeting greeting = (Greeting) record.value();
                    var sendingTime = LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(greeting.getTime()), TimeZone.getDefault().toZoneId());
                    System.out.printf("Received a message: greeting=%s, time=%s%n", greeting.getText(), sendingTime);
                }
            }
        } finally {
            System.out.println("Closing consumer");
            consumer.close();
        }
    }

}
