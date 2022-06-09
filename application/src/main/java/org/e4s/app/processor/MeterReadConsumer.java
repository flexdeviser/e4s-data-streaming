package org.e4s.app.processor;

import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.e4s.model.MeterRead;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

@Component
public class MeterReadConsumer {

    Logger LOG = LoggerFactory.getLogger(MeterReadConsumer.class);
    private static final Serde<UUID> UUID_SERDE = Serdes.UUID();
    JsonSerializer<MeterRead> meterReadJsonSerializer = new JsonSerializer<>();
    JsonDeserializer<MeterRead> meterReadJsonDeserializer = new JsonDeserializer<>(MeterRead.class);
    Serde<MeterRead> METER_READ_SERDE = Serdes.serdeFrom(meterReadJsonSerializer, meterReadJsonDeserializer);

    @Value(value = "${spring.kafka.topic}")
    private String topic;


    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        // create stream
        KStream<UUID, MeterRead> messageStream = streamsBuilder
                .stream(topic, Consumed.with(UUID_SERDE, METER_READ_SERDE));
        // log message
        messageStream.foreach((k, v) -> LOG.info("new message from topic: {} key: {}, value: {}", topic, k, v));
    }


}
