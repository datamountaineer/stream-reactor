package io.lenses.java.streamreactor.connect.azure.eventhubs.config;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;

/**
 * Class to indicate what kind of data is being received from Kafka Consumer.
 */
@Getter
public enum SourceDataType {

  BYTE(ByteArrayDeserializer.class, Schema.OPTIONAL_BYTES_SCHEMA);

  private final Class<? extends Deserializer> deserializerClass;
  private final Schema schema;
  private static final Map<String, SourceDataType> NAME_TO_DATA_SERIALIZER_TYPE;

  static {
    NAME_TO_DATA_SERIALIZER_TYPE =
        Arrays.stream(values()).collect(Collectors.toMap(Enum::name, Function.identity()));
  }

  SourceDataType(Class<? extends Deserializer> deserializerClass, Schema schema) {
    this.deserializerClass = deserializerClass;
    this.schema = schema;
  }

  public static SourceDataType fromName(String name) {
    return NAME_TO_DATA_SERIALIZER_TYPE.get(name.toUpperCase());
  }

  /**
   * Class indicates what data types are being transferred by Task.
   */
  @Getter
  public static class KeyValueTypes {
    private final SourceDataType keyType;
    private final SourceDataType valueType;
    public static final KeyValueTypes DEFAULT_TYPES = new KeyValueTypes(BYTE, BYTE);

    public KeyValueTypes(SourceDataType keyType, SourceDataType valueType) {
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KeyValueTypes that = (KeyValueTypes) o;
      return keyType == that.keyType && valueType == that.valueType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyType, valueType);
    }
  }
}
