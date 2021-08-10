package com.jeancsil.ktail.service;

import com.jeancsil.ktail.models.KafkaServer;
import com.jeancsil.ktail.serializer.KafkaProtobufSerializer;
import com.jeancsil.protos.Place;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class PlacesKafkaProducer {
    private final KafkaProducer<Integer, Place> producer;

    public PlacesKafkaProducer(List<KafkaServer> servers) {
        producer = createProducer(servers);
    }

    public KafkaProducer<Integer, Place> getProducer() {
        return producer;
    }

    private KafkaProducer<Integer, Place> createProducer(List<KafkaServer> servers) {
        var bootstrapServers = String.join(
                ",",
                Arrays.asList(servers.stream().map(KafkaServer::host).collect(Collectors.joining())));

        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCountryProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);

        return new KafkaProducer<>(props);
    }
}
