package com.jeancsil.ktail.consumer;

import com.jeancsil.ktail.serializer.KafkaProtobufDeserializer;
import com.jeancsil.protos.Place;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@Service
public class PlacesKafkaConsumer {
    private final Consumer<Integer, Place> consumer;

    public PlacesKafkaConsumer() {
        consumer = createConsumer();
    }

    public Consumer<Integer, Place> getConsumer() {
        return consumer;
    }

    private Consumer<Integer, Place> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KtailConsumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<Integer, Place> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(System.getenv("ktail.kafka.topic")));
        return consumer;
    }

    //TODO REMOVE this duplication
    private String getBootstrapServers() {
        final var receivedBrokers = System.getenv("ktail.kafka.brokers");
        log.info("Using the following Kafka brokers: " + receivedBrokers);
        return receivedBrokers;
    }
}
