package org.example.entity;

import lombok.Data;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;

@Data
@DefaultCoder(SerializableCoder.class)
public class EventSubject implements Serializable {

    private Long id;
    private String type;
}
