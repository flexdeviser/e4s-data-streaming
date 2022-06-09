package org.e4s.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class MeterRead {

    @JsonProperty
    public int voltage;

    @JsonProperty
    public int current;

    public MeterRead() {
    }

    public MeterRead(final int voltage, final int current) {
        this.voltage = voltage;
        this.current = current;
    }


}
