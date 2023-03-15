package com.pshakhlovich.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class GreetingKafkaProducer {
    private final String topic;
    private final KafkaProducer<String, Greeting> kafkaProducer;

    public GreetingKafkaProducer(String bootstrapServers, String topic, String schemaRegistry) {
        System.out.println("Initializing Producer");
        this.topic = topic;
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);

        kafkaProducer = new KafkaProducer<>(props);
    }

    public void produce() {
        var greeting = Greeting.newBuilder()
                .setText("Hello!")
                .setTime(System.currentTimeMillis())
                .build();

        var record = new ProducerRecord<String, Greeting>(topic, greeting);

        kafkaProducer.send(record, ((metadata, exception) ->
                System.out.printf(
                        "Produced record to topic %s partition %s at offset %s%n",
                        metadata.topic(), metadata.partition(), metadata.offset())));
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutDown() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
