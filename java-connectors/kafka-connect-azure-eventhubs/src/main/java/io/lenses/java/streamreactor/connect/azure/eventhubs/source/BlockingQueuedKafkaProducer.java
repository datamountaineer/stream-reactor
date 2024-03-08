package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Abstraction over Kafka {@link Consumer} class that wraps the Consumer into a thread and allows
 * it to output its records into a {@link BlockingQueue} shared with {@link EventHubsKafkaConsumerController}.
 */
@Slf4j
public class BlockingQueuedKafkaProducer implements BlockingQueueProducer {
  private static final Duration DEFAULT_POLL_DURATION =  Duration.of(1, ChronoUnit.SECONDS);
  private final TopicPartitionOffsetProvider topicPartitionOffsetProvider;
  private final BlockingQueue<ConsumerRecords<String, String>> recordsQueue;
  private final Consumer<String, String> consumer;
  private final String clientId;
  private final String topic;
  private final boolean shouldSeekToLatest;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * Class is a proxy that allows access to some methods of Kafka Consumer. It's main purpose is to
   * create a thread around the consumer and put consumer record into BlockingQueue. After that it
   * starts consumption.
   *
   * @param topicPartitionOffsetProvider TopicPartitionOffsetProvider for subscription handler
   * @param recordsQueue                 BlockingQueue to put records into
   * @param consumer                     Kafka Consumer
   * @param clientId                     consumer client id
   * @param topic                        kafka topic to consume from
   * @param shouldSeekToLatest           informs where should consumer seek when
   *                                     there are no offsets committed
   */
  public BlockingQueuedKafkaProducer(TopicPartitionOffsetProvider topicPartitionOffsetProvider,
      BlockingQueue<ConsumerRecords<String, String>> recordsQueue,
      Consumer<String, String> consumer, String clientId, String topic, boolean shouldSeekToLatest) {
    this.topicPartitionOffsetProvider = topicPartitionOffsetProvider;
    this.recordsQueue = recordsQueue;
    this.consumer = consumer;
    this.clientId = clientId;
    this.topic = topic;
    this.shouldSeekToLatest = shouldSeekToLatest;

    start();
  }

  /**
   * Starts the production to the records queue.
   */
  public void start() {
    if (!initialized.getAndSet(true)) {
      Thread pollingThread = new Thread(new EventhubsPollingRunnable());

      pollingThread.start();
      initialized.set(true);
    }
  }

  public void stop(Duration timeoutDuration) {
    consumer.close(timeoutDuration);
    running.set(false);
  }

  private class EventhubsPollingRunnable implements Runnable {

    @Override
    public void run() {
      running.set(true);
      log.info("Subscribing to topic: {}", topic);
      consumer.subscribe(Collections.singletonList(topic),
          new AzureConsumerRebalancerListener(topicPartitionOffsetProvider, consumer, shouldSeekToLatest));
      while (running.get()) {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(DEFAULT_POLL_DURATION);
        if (consumerRecords != null && !consumerRecords.isEmpty()) {
          try {
            boolean offer = false;
            while (!offer) {
              offer = recordsQueue.offer(consumerRecords, 5, TimeUnit.SECONDS);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Kafka Consumer with clientId={} has been interrupted on offering", clientId);
          }
        }
      }
    }
  }
}