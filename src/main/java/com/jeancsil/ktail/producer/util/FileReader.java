package com.jeancsil.ktail.producer.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class FileReader {
    public List<String> readFile(String filename) throws FileNotFoundException {
        try {
            return Files.readAllLines(Path.of(filename), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FileNotFoundException(String.format("File %s is not accessible.", e.getMessage()));
        }
    }
}
