package org.e4s.datapump;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.e4s.model.MeterRead;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
    Logger LOG = LoggerFactory.getLogger(AppLauncher.class);

    public static void main(String[] args) {
        SpringApplication.run(AppLauncher.class, args);
    }

    @Value(value = "${spring.kafka.topic}")
    private String topic;

    @Autowired
    private KafkaTemplate<UUID, MeterRead> kafkaTemp;


    private ArrayBlockingQueue<MessageRequest> inputQueue = new ArrayBlockingQueue<>(100);

    final ExecutorService messageMaker = Executors.newSingleThreadExecutor();
    final ExecutorService messageSender = Executors.newSingleThreadExecutor();

    /**
     * for test only
     *
     * @return
     */
    @Bean
    public ApplicationRunner runner() {
        final Logger LOG = LoggerFactory.getLogger("Kafka.Producer.Runner");

        return args -> {
            List<UUID> ids = new ArrayList<>();
            // generate 10 uuid
            IntStream.rangeClosed(0, 9).forEach(i -> ids.add(i, UUID.nameUUIDFromBytes(String.valueOf(i).getBytes())));
            final Random random = new Random();

            final LocalDate localDate = LocalDate.now();
            //Local date time
            final LocalDateTime startOfDay = localDate.atStartOfDay();

            final long startMilli = startOfDay.toEpochSecond(ZoneOffset.UTC) * 1000;

            final Map<UUID, Long> latestTs = new HashMap<>();


            // generate message and put on queue
            messageMaker.submit(() -> {

                int count = 0;
                while (true) {
                    // get random id
                    final int randomElementIndex = ThreadLocalRandom.current().nextInt(ids.size()) % ids.size();

                    Long lastTs = latestTs.get(ids.get(randomElementIndex));
                    if (lastTs == null) {
                        lastTs = startMilli;
                    } else {
                        lastTs += 60 * 1000 * 5;
                    }

                    // update
                    latestTs.put(ids.get(randomElementIndex), lastTs);

                    final MessageRequest request = new MessageRequest(ids.get(randomElementIndex), new MeterRead(
                            random.ints(220, 260).findFirst().getAsInt(),
                            random.ints(1, 4).findFirst().getAsInt(),
                            lastTs
                    ));

                    while (!inputQueue.offer(request)) {
                        try {
                            LOG.warn("input queue is full, wait for 5 seconds");
                            SECONDS.sleep(5);
                        } catch (InterruptedException e) {
                            LOG.error("Sleep thread interrupted.", e);
                            throw e;
                        }
                    }

                    LOG.info("put message on input queue");
                    if (count == 50) {
                        // sleep 10 seconds
                        SECONDS.sleep(10);
                        count = 0;
                    } else {
                        count++;
                    }


                }
            });


            // sender
            messageSender.submit(() -> {

                while (true) {
                    final MessageRequest request = inputQueue.take();
                    kafkaTemp.send(topic, request.getId(), request.getRead()).addCallback(new ListenableFutureCallback<>() {
                        @Override
                        public void onFailure(final Throwable ex) {
                            LOG.error("not able to send data to Kafka", ex);
                        }

                        @Override
                        public void onSuccess(final SendResult<UUID, MeterRead> result) {
                            LOG.info("send message with key [{}] to topic [{}] successfully.", result.getProducerRecord().key(), result.getProducerRecord().topic());
                        }
                    });
                    if (inputQueue.size() % 10 == 0) {
                        LOG.info("input queue size: {}", inputQueue.size());
                    }


                    MILLISECONDS.sleep(500);
                }
            });
        };
    }

    @PostConstruct
    public void init() {
        // Setting Spring Boot SetTimeZone
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        final Calendar calendar = Calendar.getInstance();
        LOG.info("Application running with TZ: {}", calendar.getTimeZone().getID());
    }

    @PreDestroy
    void exit() throws InterruptedException {
        LOG.info("Wait for all messages send to Topic");
        messageMaker.shutdownNow();
        messageSender.shutdown();
        while (!inputQueue.isEmpty()) {
            SECONDS.sleep(5);
        }
        kafkaTemp.destroy();
        LOG.info("Application exit.");
    }


}
