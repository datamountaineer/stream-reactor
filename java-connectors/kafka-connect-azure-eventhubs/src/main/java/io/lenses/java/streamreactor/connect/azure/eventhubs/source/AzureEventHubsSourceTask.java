package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants.EVENTHUB_NAME;
import static java.util.Optional.ofNullable;

import io.lenses.java.streamreactor.common.util.JarManifest;
import io.lenses.java.streamreactor.common.util.StringUtils;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * Implementation of {@link SourceTask} for Microsoft Azure EventHubs.
 */
@Slf4j
public class AzureEventHubsSourceTask extends SourceTask {

  private Duration closeTimeout;
  private static final int RECORDS_QUEUE_DEFAULT_SIZE = 10;
  private final JarManifest jarManifest;
  private EventHubsKafkaConsumerController eventHubsKafkaConsumerController;
  private BlockingQueueProducerProvider blockingQueueProducerProvider;

  public AzureEventHubsSourceTask() {
    jarManifest = new JarManifest(getClass().getProtectionDomain().getCodeSource().getLocation());
  }

  public AzureEventHubsSourceTask(JarManifest jarManifest) {
    this.jarManifest = jarManifest;
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    OffsetStorageReader offsetStorageReader = ofNullable(this.context).flatMap(
        context -> ofNullable(context.offsetStorageReader())).orElseThrow();
    AzureEventHubsConfig azureEventHubsConfig = new AzureEventHubsConfig(props);
    TopicPartitionOffsetProvider topicPartitionOffsetProvider = new TopicPartitionOffsetProvider(offsetStorageReader);

    ArrayBlockingQueue<ConsumerRecords<byte[], byte[]>> recordsQueue = new ArrayBlockingQueue<>(
        RECORDS_QUEUE_DEFAULT_SIZE);
    blockingQueueProducerProvider = new BlockingQueueProducerProvider(topicPartitionOffsetProvider);
    KafkaByteBlockingQueuedProducer producer = blockingQueueProducerProvider.createProducer(
        azureEventHubsConfig, recordsQueue);
    List<String> outputTopics = getOutputTopicsFromConfig(azureEventHubsConfig);
    EventHubsKafkaConsumerController kafkaConsumerController = new EventHubsKafkaConsumerController(
        producer, recordsQueue, outputTopics);
    initialize(kafkaConsumerController, azureEventHubsConfig);
  }

  /**
   * Initializes the Task. This method shouldn't be called if start() was already called with
   * {@link EventHubsKafkaConsumerController} instance.
   *
   * @param eventHubsKafkaConsumerController {@link EventHubsKafkaConsumerController} for this task
   * @param azureEventHubsConfig config for task
   */
  public void initialize(EventHubsKafkaConsumerController eventHubsKafkaConsumerController,
      AzureEventHubsConfig azureEventHubsConfig) {
    this.eventHubsKafkaConsumerController = eventHubsKafkaConsumerController;
    closeTimeout =
        Duration.of(azureEventHubsConfig.getInt(AzureEventHubsConfigConstants.CONSUMER_CLOSE_TIMEOUT),
            ChronoUnit.SECONDS);
    log.info("{} initialised.", getClass().getSimpleName());
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> poll = eventHubsKafkaConsumerController.poll(
        Duration.of(1, ChronoUnit.SECONDS));
    return poll.isEmpty() ? null : poll;
  }

  @Override
  public void stop() {
    ofNullable(eventHubsKafkaConsumerController)
        .ifPresent(consumerController -> consumerController.close(closeTimeout));
  }

  /**
   * Returns output topics (if specified in config) or just eventHub name (in case no one specified it)
   * which will mean mirroring source to kafka.
   *
   * @param azureEventHubsConfig task configuration
   * @return output topics list
   */
  private List<String> getOutputTopicsFromConfig(AzureEventHubsConfig azureEventHubsConfig) {
    List<String> outputTopics =
        Arrays.stream(azureEventHubsConfig.getString(AzureEventHubsConfigConstants.OUTPUT_TOPICS)
            .split(",")).filter(str -> !StringUtils.isBlank(str)).collect(Collectors.toList());
    return outputTopics.isEmpty()
        ? Collections.singletonList(azureEventHubsConfig.getString(EVENTHUB_NAME)) : outputTopics;
  }
}
