package com.jeancsil.ktail.args;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Arguments {
  @Parameter(
      names = {"--file", "-f"},
      description = "File to read the data to be produced to Kafka.")
  private String file;

  public String getFile() {
    return file;
  }

  public static Arguments parseArguments(String[] args) {
    final var arguments = new Arguments();
    JCommander.newBuilder().addObject(arguments).build().parse(args);

    return arguments;
  }
}
