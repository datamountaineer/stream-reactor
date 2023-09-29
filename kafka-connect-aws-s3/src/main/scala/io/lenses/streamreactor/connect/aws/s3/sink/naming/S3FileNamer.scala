/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.aws.s3.sink.naming

import io.lenses.streamreactor.connect.cloud.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.sink.config.padding.PaddingStrategy

trait S3FileNamer {
  def fileName(
    topicPartitionOffset: TopicPartitionOffset,
  ): String
}
class OffsetS3FileNamer(
  offsetPaddingStrategy: PaddingStrategy,
  extension:             String,
) extends S3FileNamer {
  def fileName(
    topicPartitionOffset: TopicPartitionOffset,
  ): String =
    s"${offsetPaddingStrategy.padString(topicPartitionOffset.offset.value.toString)}.$extension"
}
class TopicPartitionOffsetS3FileNamer(
  partitionPaddingStrategy: PaddingStrategy,
  offsetPaddingStrategy:    PaddingStrategy,
  extension:                String,
) extends S3FileNamer {
  def fileName(
    topicPartitionOffset: TopicPartitionOffset,
  ): String =
    s"${topicPartitionOffset.topic.value}(${partitionPaddingStrategy.padString(
      topicPartitionOffset.partition.toString,
    )}_${offsetPaddingStrategy.padString(topicPartitionOffset.offset.value.toString)}).$extension"

}
