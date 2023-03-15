package com.pshakhlovich.avro;

import java.util.concurrent.Executors;

public class KafkaAvroApp {

    public static void main(String[] args) {
        var bootstrapServer = "http://localhost:9092";
        var topic = "avro-kafka";
        var schemaRegistry = "http://localhost:8081";

        var producer = new GreetingKafkaProducer(bootstrapServer, topic, schemaRegistry);
        var consumer = new GreetingKafkaConsumer(bootstrapServer, topic, schemaRegistry);

        var executor = Executors.newSingleThreadExecutor();

        try {
            for (int i = 0; i < 10; i++) {
                executor.execute(producer::produce);
            }
            consumer.consume();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.shutDown();
        }
    }
}