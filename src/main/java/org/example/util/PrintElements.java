package org.example.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.example.entity.EventStatistic;

public class PrintElements extends DoFn<KV<String, EventStatistic>, KV<String, EventStatistic>> {

    // print all elements in console
    @ProcessElement
    public void processElement(@Element KV<String, EventStatistic> element) {
        System.out.println(element.getKey() + ": " + element.getValue());
    }

}
