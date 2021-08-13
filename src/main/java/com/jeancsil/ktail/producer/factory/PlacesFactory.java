package com.jeancsil.ktail.producer.factory;

import com.jeancsil.protos.Place;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class PlacesFactory {
  public List<Place> create(List<String> fileContent) {
    final var places = new ArrayList<Place>(fileContent.size());

    fileContent
        .iterator()
        .forEachRemaining(
            line -> {
              final var data = line.split("\\t");
              places.add(Place.newBuilder().setGeonameId(Integer.parseInt(data[0])).setName(data[1]).build());
            });
    return places;
  }
}
