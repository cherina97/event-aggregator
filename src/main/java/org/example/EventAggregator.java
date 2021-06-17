package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.example.entity.Event;
import org.example.entity.EventStatistic;
import org.example.option.Option;
import org.example.util.ParseJson;
import org.example.util.ParseToStatistic;
import org.example.util.PrintElements;

public class EventAggregator {
    public static void main(String[] args) {
        Option option = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(Option.class);

        run(option);
    }

    private static void run(Option option) {
        Pipeline p = Pipeline.create(option);

        //reading json
        PCollection<String> rows = p.apply(TextIO.read().from(option.getInputFiles()));

        //parse json lines into event obj
        PCollection<Event> eventCollection = rows.apply(ParDo.of(new ParseJson()));

        //create KV<String(city), Event>
        PCollection<KV<String, Event>> kvpCollection = eventCollection.apply(WithKeys.of(Event::getCity))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Event.class)));

        //create KV<String(city), List<Event>>, grouping by city
        PCollection<KV<String, Iterable<Event>>> cityEvents = kvpCollection.apply(GroupByKey.create())
                .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(SerializableCoder.of(Event.class))));

        //parse List<Event> to EventStatistic
        PCollection<KV<String, EventStatistic>> resultStatistic = cityEvents.apply(ParDo.of(new ParseToStatistic()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(EventStatistic.class)));

        //print elements in console
        resultStatistic.apply(ParDo.of(new PrintElements()));

        //write in .avro
        resultStatistic.apply(
                FileIO.<String, KV<String, EventStatistic>>writeDynamic()
                        .by(KV::getKey)
                        .via(Contextful.fn((SerializableFunction<KV<String, EventStatistic>, EventStatistic>) KV::getValue),
                                AvroIO.sink(EventStatistic.class))
//                      .to(String.format("gs://bucket/data%s/", suffix)
                        .to(option.getOutput())
                        .withNaming(cityName -> FileIO.Write.defaultNaming(cityName, ".avro"))
                        .withDestinationCoder(StringUtf8Coder.of())
                        .withNumShards(1));

        p.run().waitUntilFinish();
    }
}
