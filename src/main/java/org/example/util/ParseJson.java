package org.example.util;

import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;
import org.example.entity.Event;

public class ParseJson extends DoFn<String, Event> {

    @ProcessElement
    public void processElement(@Element String string, OutputReceiver<Event> outputReceiver){
        Event event = new Gson().fromJson(string, Event.class);
        outputReceiver.output(event);
    }
}
