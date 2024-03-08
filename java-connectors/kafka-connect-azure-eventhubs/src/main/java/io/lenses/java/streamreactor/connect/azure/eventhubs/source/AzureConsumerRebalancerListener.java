package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import io.lenses.java.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureOffsetMarker;
import io.lenses.java.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureTopicPartitionKey;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * This class is an implementation of {@link ConsumerRebalanceListener} that can be used to provide
 * OnlyOnce support and seek consumers into relevant offsets if needed.
 */
@Slf4j
public class AzureConsumerRebalancerListener implements ConsumerRebalanceListener {

  private final boolean shouldSeekToLatest;
  private final TopicPartitionOffsetProvider topicPartitionOffsetProvider;
  private final Consumer<?, ?> kafkaConsumer;

  /**
   * Constructs {@link AzureConsumerRebalancerListener} for particular Kafka Consumer.
   *
   * @param topicPartitionOffsetProvider provider of committed offsets
   * @param kafkaConsumer Kafka Consumer
   * @param shouldSeekToLatest informs whether we should seek to latest or earliest if no offsets found
   */
  public AzureConsumerRebalancerListener(
      TopicPartitionOffsetProvider topicPartitionOffsetProvider,
      Consumer<?, ?> kafkaConsumer, boolean shouldSeekToLatest) {
    this.topicPartitionOffsetProvider = topicPartitionOffsetProvider;
    this.kafkaConsumer = kafkaConsumer;
    this.shouldSeekToLatest = shouldSeekToLatest;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    // implementation not needed, offsets already committed
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    partitions.forEach(partition -> {
      AzureTopicPartitionKey partitionKey = new AzureTopicPartitionKey(
          partition.topic(), partition.partition());
      Optional<AzureOffsetMarker> partitionOffset = topicPartitionOffsetProvider.getOffset(partitionKey);
      partitionOffset.ifPresentOrElse(
          offset -> kafkaConsumer.seek(partition, offset.getOffsetValue()),
          () -> {
            if (shouldSeekToLatest) {
              kafkaConsumer.seekToEnd(Collections.singletonList(partition));
            } else {
              kafkaConsumer.seekToBeginning(Collections.singletonList(partition));
            }
          });
    });
  }

}