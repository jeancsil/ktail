package com.jeancsil.ktail;

import com.beust.jcommander.JCommander;
import com.jeancsil.ktail.args.Arguments;
import com.jeancsil.ktail.factory.PlacesFactory;
import com.jeancsil.ktail.models.KafkaServer;
import com.jeancsil.ktail.service.PlacesKafkaProducer;
import com.jeancsil.ktail.service.PlacesService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerApplication {
  public static void main(String[] args) {
    log.info("Starting the application");

    final var arguments = new Arguments();
    JCommander.newBuilder().addObject(arguments).build().parse(args);

    final var placesService = new PlacesService(new PlacesFactory());

    final var producer =
        new PlacesKafkaProducer(List.of(new KafkaServer("0.0.0.0:9092"))).getProducer();

    placesService
        .getPlaces(arguments.getFile())
        .iterator()
        .forEachRemaining(
            place -> {
              try {
                final var record = new ProducerRecord<>("places", place.getGeonameId(), place);

                RecordMetadata metadata = producer.send(record).get();

                if (log.isTraceEnabled()) {
                    log.trace(
                            "key: "
                                    + record.key()
                                    + " value: "
                                    + record.value()
                                    + " partition: "
                                    + metadata.partition()
                                    + " offset: "
                                    + metadata.offset());
                }
              } catch (InterruptedException | ExecutionException e) {
                log.error(e.getMessage(), e);
              }

              if (log.isTraceEnabled()) {
                  log.trace("geonameId: " + place.getGeonameId() + " name: " + place.getName());
              }
            });

    producer.flush();
    producer.close();
  }
}
