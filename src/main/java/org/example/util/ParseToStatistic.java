package org.example.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.example.entity.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ParseToStatistic extends DoFn<KV<String, Iterable<Event>>, KV<String, EventStatistic>> {

    // List<Event> ---> EventStatistic
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<Event>> cityEvent, OutputReceiver<KV<String, EventStatistic>> out) {

        Map<EventSubject, List<Event>> eventsByEventSubject = StreamSupport
                .stream(Objects.requireNonNull(cityEvent.getValue()).spliterator(), false)
                .collect(Collectors.groupingBy(Event::getEventSubject));

        List<EventStatisticSubject> eventStatisticSubjects = eventsByEventSubject.keySet()
                .stream()
                .map(eventSubject -> {
                    List<Event> eventsBySubject = eventsByEventSubject.get(eventSubject);

                    Map<String, List<Event>> eventsGroupByEventType = eventsBySubject
                            .stream()
                            .collect(Collectors.groupingBy(Event::getEventType));

                    List<EventStatisticSubjectActivities> eventStatisticSubjectActivities = eventsGroupByEventType
                            .keySet()
                            .stream()
                            .map(eventType -> new EventStatisticSubjectActivities(
                                    eventType,
                                    randInt7(),
                                    randInt7(),
                                    randInt30(),
                                    randInt30()
                            )).collect(Collectors.toList());

                    return new EventStatisticSubject(
                            eventSubject.getId(),
                            eventSubject.getType(),
                            eventStatisticSubjectActivities);

                }).collect(Collectors.toList());

        String city = cityEvent.getKey();
        EventStatistic eventStatistic = new EventStatistic(eventStatisticSubjects);
        out.output(KV.of(city, eventStatistic));
    }

    public static int randInt7() {
        return new Random().nextInt(7);
    }

    public static int randInt30() {
        return new Random().nextInt(30);
    }
}
