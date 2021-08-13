package com.jeancsil.ktail;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jeancsil.ktail.di.ContainerWrapper;
import com.jeancsil.ktail.producer.PlacesKafkaProducer;
import com.jeancsil.ktail.producer.args.Arguments;
import com.jeancsil.ktail.producer.service.PlacesService;
import com.jeancsil.protos.Place;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;

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
                  (metadata, exception) -> logKafkaMessage(record, metadata, exception));
            });

    log.info("Flushing the producer...");
    producer.flush();
    log.info("Closing the producer (wait for 5 minutes)...");
    producer.close(Duration.ofMinutes(5));
    log.info("Stopping the application...");
  }

  // TODO extract this to a class and reuse in the consumer
  private static void logKafkaMessage(
      ProducerRecord<Integer, Place> record, RecordMetadata metadata, Exception exception) {
    if (metadata != null) {
      try {
        log.info(JsonFormat.printer().print(record.value()));
      } catch (InvalidProtocolBufferException e) {
        log.error("Impossible to print the message: " + e.getMessage());
      }
    } else {
      log.error(exception.getMessage());
    }
  }
}
