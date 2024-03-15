package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig.getPrefixedKafkaConsumerConfigKey;
import static io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants.EVENTHUB_NAME;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.SourceDataType.KeyValueTypes;
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
public class BlockingQueueProducerProvider implements ProducerProvider<byte[], byte[]> {

  private static final boolean STRIP_PREFIX = true;
  private static final String EARLIEST_OFFSET = "earliest";
  private static final String LATEST_OFFSET = "latest";
  private static final String CONSUMER_OFFSET_EXCEPTION_MESSAGE =
      "allowed values are: earliest/latest";
  private final TopicPartitionOffsetProvider topicPartitionOffsetProvider;


  public BlockingQueueProducerProvider(TopicPartitionOffsetProvider topicPartitionOffsetProvider) {
    this.topicPartitionOffsetProvider = topicPartitionOffsetProvider;
  }

  /**
   * Instantiates BlockingQueuedKafkaConsumer from given properties.
   *
   * @param azureEventHubsConfig Config of Task
   * @param recordBlockingQueue  BlockingQueue for ConsumerRecords
   * @return BlockingQueuedKafkaConsumer instance.
   */
  public KafkaByteBlockingQueuedProducer createProducer(AzureEventHubsConfig azureEventHubsConfig,
      BlockingQueue<ConsumerRecords<byte[], byte[]>> recordBlockingQueue) {
    String connectorName = azureEventHubsConfig.getString(AzureEventHubsConfigConstants.CONNECTOR_NAME);
    final String clientId = connectorName + "#" + UUID.randomUUID();
    log.info("Attempting to create Client with Id:{}", clientId);
    KeyValueTypes keyValueTypes = KeyValueTypes.DEFAULT_TYPES;

    Map<String, Object> consumerProperties = azureEventHubsConfig.originalsWithPrefix(
        AzureEventHubsConfigConstants.CONNECTOR_WITH_CONSUMER_PREFIX, STRIP_PREFIX);

    consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, azureEventHubsConfig.getString(
            getPrefixedKafkaConsumerConfigKey(GROUP_ID_CONFIG)));
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            keyValueTypes.getKeyType().getDeserializerClass());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        keyValueTypes.getValueType().getDeserializerClass());
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

    boolean shouldSeekToLatest = shouldConsumerSeekToLatest(azureEventHubsConfig);
    String topic = azureEventHubsConfig.getString(EVENTHUB_NAME);

    return new KafkaByteBlockingQueuedProducer(topicPartitionOffsetProvider, recordBlockingQueue,
        kafkaConsumer, keyValueTypes, clientId, topic, shouldSeekToLatest);
  }

  private boolean shouldConsumerSeekToLatest(AzureEventHubsConfig azureEventHubsConfig) {
    String seekValue = azureEventHubsConfig.getString(AzureEventHubsConfigConstants.CONSUMER_OFFSET);
    if (EARLIEST_OFFSET.equalsIgnoreCase(seekValue)) {
      return false;
    } else if (LATEST_OFFSET.equalsIgnoreCase(seekValue)) {
      return true;
    }
    throw new ConfigException(AzureEventHubsConfigConstants.CONSUMER_OFFSET, seekValue,
        CONSUMER_OFFSET_EXCEPTION_MESSAGE);
  }
}
