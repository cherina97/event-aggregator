package org.example.util;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

public class GenerateAvsc {
    public static void main(String[] args) throws IOException {
        Schema eventStatisticSubjectActivities = SchemaBuilder.record("EventStatisticSubjectActivities")
                .namespace("org.example.entity")
                .fields()
                .requiredString("type")
                .requiredInt("past7daysCount")
                .requiredInt("past7daysUniqueCount")
                .requiredInt("past30daysCount")
                .requiredInt("past30daysUniqueCount")
                .endRecord();

        Schema eventStatisticSubject = SchemaBuilder.record("EventStatisticSubject")
                .namespace("org.example.entity")
                .fields()
                .requiredLong("id")
                .requiredString("type")
                .name("activities")
                .type().array().items().type(eventStatisticSubjectActivities).noDefault()
                .endRecord();

        Schema eventStatistic = SchemaBuilder.record("EventStatistic")
                .namespace("org.example.entity")
                .fields()
                .name("subjects")
                .type().array().items().type(eventStatisticSubject).noDefault()
                .endRecord();

        saveAvscToFile(eventStatistic.toString(), "EventStatistic");
    }

    public static void saveAvscToFile(String data, String filename) throws IOException {
        final Path resourcePath = Paths.get("src", "main", "resources");

        if (Files.notExists(Paths.get(resourcePath.toString(), "avro")))
            Files.createDirectory(Paths.get(resourcePath.toString(), "avsc"));

        Path filePath = Paths.get(Paths.get(resourcePath.toString(), "avsc").toString(), filename + ".avsc");

        if (Files.notExists(filePath))
            Files.createFile(filePath);

        Files.write(filePath, Collections.singleton(data));
    }

    //java -jar jars/avro-tools-1.10.2.jar compile schema src/main/resources/avsc/EventStatistic.avsc src
}
