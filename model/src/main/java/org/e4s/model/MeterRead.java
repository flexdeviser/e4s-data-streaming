package org.e4s.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class MeterRead {

    @JsonProperty
    public int voltage;

    @JsonProperty
    public int current;

    @JsonProperty
    public long timestamp;

    public MeterRead() {
    }

    public MeterRead(final int voltage, final int current, final long timestamp) {
        this.voltage = voltage;
        this.current = current;
        this.timestamp = timestamp;
    }


}
