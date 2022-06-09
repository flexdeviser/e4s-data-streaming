package org.e4s.datapump;

import java.util.UUID;

import org.e4s.model.MeterRead;

import lombok.Data;

@Data
public class MessageRequest {
    private UUID id;

    private MeterRead read;

    public MessageRequest(final UUID id, final MeterRead read) {
        this.id = id;
        this.read = read;
    }
}
