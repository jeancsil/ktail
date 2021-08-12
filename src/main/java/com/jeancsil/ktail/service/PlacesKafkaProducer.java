package com.jeancsil.ktail.service;

import com.jeancsil.ktail.serializer.KafkaProtobufSerializer;
import com.jeancsil.protos.Place;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
@Slf4j
public class PlacesKafkaProducer {
  private final KafkaProducer<Integer, Place> producer;

  public PlacesKafkaProducer() {
    producer = createProducer();
  }

  public KafkaProducer<Integer, Place> getProducer() {
    return producer;
  }

  //TODO thread possible exceptions -> Throw a specific exception with proper name.
  private String getBootstrapServers() {
    final var receivedBrokers = System.getenv("kafka.brokers");
    log.info("Using the following Kafka brokers: " + receivedBrokers);
    return receivedBrokers;
  }

  private KafkaProducer<Integer, Place> createProducer() {
    var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCountryProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);

    return new KafkaProducer<>(props);
  }
}
