package org.e4s.datapump;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import org.e4s.model.MeterRead;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication
public class AppLauncher {

    public static void main(String[] args) {
        SpringApplication.run(AppLauncher.class, args);
    }

    @Value(value = "${spring.kafka.topic}")
    private String topic;

    /**
     * for test only
     * @param kafkaTemp
     * @return
     */
    @Bean
    public ApplicationRunner runner(KafkaTemplate<UUID, MeterRead> kafkaTemp) {
        Logger LOG = LoggerFactory.getLogger("Kafka.Producer.Runner");
        return args -> {
            List<UUID> ids = new ArrayList<>();
            // generate 10 uuid
            IntStream.rangeClosed(0, 9).forEach(i -> ids.add(i, UUID.nameUUIDFromBytes(String.valueOf(i).getBytes())));

            while (true) {
                ids.forEach(id -> kafkaTemp.send(topic, id, new MeterRead(240, 1)).addCallback(new ListenableFutureCallback<>() {

                    @Override
                    public void onSuccess(final SendResult<UUID, MeterRead> result) {
                        LOG.info("send message with key [{}] to topic [{}] successfully.", result.getProducerRecord().key(), result.getProducerRecord().topic());
                    }

                    @Override
                    public void onFailure(final Throwable ex) {
                        LOG.error("not able to send data to Kafka", ex);
                    }
                }));
                // sleep 10 seconds
                Thread.sleep(SECONDS.toMillis(10));
            }
        };
    }

}
