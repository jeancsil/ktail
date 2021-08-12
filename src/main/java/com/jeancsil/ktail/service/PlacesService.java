package com.jeancsil.ktail.service;

import com.jeancsil.ktail.factory.PlacesFactory;
import com.jeancsil.ktail.util.FileReader;
import com.jeancsil.protos.Place;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.util.List;

@Slf4j
@Service
public record PlacesService(PlacesFactory factory) {

    public List<Place> getPlaces(final String fileName) {
        try {
            log.info("Reading: " + fileName);
            final var fileContent = new FileReader().readFile(fileName);
            return factory.create(fileContent);

        } catch (FileNotFoundException e) {
            log.error(e.getMessage());
            throw new IllegalStateException();//TODO
        }
    }
}
