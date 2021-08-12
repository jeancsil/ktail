package com.jeancsil.ktail;

import com.jeancsil.ktail.args.Arguments;
import com.jeancsil.ktail.di.ContainerWrapper;
import com.jeancsil.ktail.producer.PlacesKafkaProducer;
import com.jeancsil.ktail.service.PlacesService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class ProducerApplication {

  public static void main(String[] args) {
    log.info("Starting the kafka producer application");
    final var container = new ContainerWrapper("com.jeancsil.ktail");
    final var arguments = Arguments.parseArguments(args);
    final var placesService = container.getBean(PlacesService.class);
    final var producer = container.getBean(PlacesKafkaProducer.class).getProducer();
    final var topic = System.getenv("ktail.kafka.topic");

    placesService
        .getPlaces(arguments.getFile())
        .iterator()
        .forEachRemaining(
            place -> {
              final var record = new ProducerRecord<>(topic, place.getGeonameId(), place);

              producer.send(
                  record,
                  (metadata, exception) -> {
                    if (metadata != null) {
                      log.trace(
                          "key: "
                              + record.key()
                              + " value: "
                              + record.value()
                              + " partition: "
                              + metadata.partition()
                              + " offset: "
                              + metadata.offset());

                    } else {
                      log.error(exception.getMessage());
                    }
                  });

              if (log.isTraceEnabled()) {
                log.trace("geonameId: " + place.getGeonameId() + " name: " + place.getName());
              }
            });

    producer.flush();
    producer.close();
    log.info("Stopping the application");
  }
}
