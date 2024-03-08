package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig.getPrefixedKafkaConsumerConfigKey;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigException;

/**
 * Provider for BlockingQueuedKafkaConsumers.
 */
@Slf4j
public class BlockingQueueProducerProvider implements ProducerProvider {

  private static final boolean STRIP_PREFIX = true;
  private static final String EARLIEST_OFFSET = "earliest";
  private static final String LATEST_OFFSET = "latest";
  private static final String CONSUMER_OFFSET_EXCEPTION_MESSAGE =
      "Allowed values for " + AzureEventHubsConfigConstants.CONSUMER_OFFSET + ": earliest/latest";
  private final TopicPartitionOffsetProvider topicPartitionOffsetProvider;


  public BlockingQueueProducerProvider(TopicPartitionOffsetProvider topicPartitionOffsetProvider) {
    this.topicPartitionOffsetProvider = topicPartitionOffsetProvider;
  }

  /**
   * Instantiates BlockingQueuedKafkaConsumer from given properties.
   *
   * @param azureEventHubsConfig Config of Task
   * @param recordBlockingQueue BlockingQueue for ConsumerRecords
   * @return BlockingQueuedKafkaConsumer instance.
   */
  public BlockingQueuedKafkaProducer createProducer(AzureEventHubsConfig azureEventHubsConfig,
      BlockingQueue<ConsumerRecords<String, String>> recordBlockingQueue) {
    String connectorName = azureEventHubsConfig.getString(AzureEventHubsConfigConstants.CONNECTOR_NAME);
    final String clientId = connectorName + "#" + UUID.randomUUID();
    log.info("Attempting to create Client with Id:{}", clientId);

    Map<String, Object> consumerProperties = azureEventHubsConfig.originalsWithPrefix(
        AzureEventHubsConfigConstants.CONNECTOR_WITH_CONSUMER_PREFIX, STRIP_PREFIX);

    consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, azureEventHubsConfig.getString(
            getPrefixedKafkaConsumerConfigKey(GROUP_ID_CONFIG)));
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        azureEventHubsConfig.getClass(
            getPrefixedKafkaConsumerConfigKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)));
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        azureEventHubsConfig.getClass(
            getPrefixedKafkaConsumerConfigKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)));
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

    boolean shouldSeekToLatest = shouldConsumerSeekToLatest(azureEventHubsConfig);
    String topic = azureEventHubsConfig.getString(AzureEventHubsConfigConstants.EVENTHUB_NAME);

    return new BlockingQueuedKafkaProducer(topicPartitionOffsetProvider, recordBlockingQueue,
        kafkaConsumer, clientId, topic, shouldSeekToLatest);
  }

  private boolean shouldConsumerSeekToLatest(AzureEventHubsConfig azureEventHubsConfig) {
    String seekValue = azureEventHubsConfig.getString(AzureEventHubsConfigConstants.CONSUMER_OFFSET);
    if (EARLIEST_OFFSET.equals(seekValue)) {
      return false;
    } else if (LATEST_OFFSET.equals(seekValue)) {
      return true;
    }
    throw new ConfigException(AzureEventHubsConfigConstants.CONSUMER_OFFSET, seekValue,
        CONSUMER_OFFSET_EXCEPTION_MESSAGE);
  }
}
