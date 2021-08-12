package com.jeancsil.ktail;

import com.jeancsil.ktail.args.Arguments;
import com.jeancsil.ktail.di.ContainerWrapper;
import com.jeancsil.ktail.producer.PlacesKafkaProducer;
import com.jeancsil.ktail.service.PlacesService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerApplication {

  public static void main(String[] args) {
    log.info("Starting the application");
    final var container = new ContainerWrapper("com.jeancsil.ktail");
    final var arguments = Arguments.parseArguments(args);
    final var placesService = container.getBean(PlacesService.class);
    final var producer = container.getBean(PlacesKafkaProducer.class).getProducer();

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
    log.info("Stopping the application");
  }
}
