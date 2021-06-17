package org.example.entity;

import lombok.Data;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;

@Data
@DefaultCoder(SerializableCoder.class)
public class Event implements Serializable {

    private int id;
    private int userId;
    private String city;
    private String eventType;
    private Long timestamp;
    private EventSubject eventSubject;
}
