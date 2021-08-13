package com.jeancsil.ktail;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jeancsil.ktail.consumer.PlacesKafkaConsumer;
import com.jeancsil.ktail.di.ContainerWrapper;
import com.jeancsil.protos.Place;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

@Slf4j
public class ConsumerApplication {

  public static void main(String[] args) {
    log.info("Starting the kafka consumer application");
    final var container = new ContainerWrapper("com.jeancsil.ktail.consumer");
    final var consumer = container.getBean(PlacesKafkaConsumer.class).getConsumer();
    //    log.info(consumer.listTopics().toString());

    final int giveUp = 100;
    int noRecordsCount = 0;

    while (true) {
      final ConsumerRecords<Integer, Place> consumerRecords =
          consumer.poll(Duration.ofMillis(1000));

      if (consumerRecords.count() == 0) {
        log.info("No records...");
        noRecordsCount++;
        if (noRecordsCount > giveUp) break;
        else continue;
      }

      consumerRecords.forEach(
          record -> {
            if (log.isTraceEnabled()) {
              log.trace(
                  "Key: "
                      + record.key()
                      + ". Value: "
                      + record.value()
                      + ". Partition: "
                      + record.partition()
                      + ". Offset: "
                      + record.offset());
            } else {
              try {
                log.info(JsonFormat.printer().print(record.value()));
              } catch (InvalidProtocolBufferException e) {
                log.error(e.getMessage());
              }
            }
          });

      consumer.commitAsync();
    }
    consumer.close();
    log.info("Stopping the kafka consumer application");
  }
}
