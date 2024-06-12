/*
 * Copyright (C) 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.bigtable;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This DoFn takes a Beam {@link Row} as input and converts to Bigtable {@link Mutation}s. Primitive
 * Cell values are serialized using the Hbase {@link Bytes} class. Complex types are expanded into
 * multiple cells.
 *
 * <p>Collections: A list or set named 'mylist' with three values will expand into three cell
 * values; 'mylist[0]', 'mylist[1]', 'mylist[2]'.
 *
 * <p>Maps: Maps follow a similar pattern but have .key or .value appended as a suffix to
 * distinguish the keys and values. As such a map called 'mymap' with two key value pairs expand
 * into four cell values: 'mymap[0].key', 'mymap[0].value', 'mymap[1].key', 'mymap[1].value'
 *
 * <p>Row values with comments starting with 'rowkeyorder' indicate that they should be part of the
 * rowkey. These fields do not get serialized as cell values but are instead joined to form the
 * Bigtable Rowkey.
 *
 * <p>The Bigtable rowkey is created by joining all row-key cells serialized as strings in order
 * with a separator specified in the constructor.
 *
 * @see Bytes
 * @see <a
 *     href="https://www.datastax.com/dev/blog/the-most-important-thing-to-know-in-cassandra-data-modeling-the-primary-key">
 *     The Cassandra Row Key </a>
 */
public class BeamRowToBigtableFn extends DoFn<Row, KV<ByteString, Iterable<Mutation>>> {

  private static final Logger LOG = LoggerFactory.getLogger(BeamRowToBigtableFn.class);

  public static final int MAX_MUTATION_PER_REQUEST = 100000;
  private static final boolean DEFAULT_SPLIT_LARGE_ROWS = false;

  private static final String ROW_KEY_DESCRIPTION_PREFIX = "rowkeyorder";
  public static final String WRITETIME_COLUMN = "writetime";
  private final ValueProvider<String> defaultColumnFamily;
  private final ValueProvider<String> defaultRowKeySeparator;
  private final ValueProvider<Boolean> splitLargeRowsFlag;
  private Boolean splitLargeRows;
  private final int maxMutationsPerRequest;
  private final ValueProvider<String> cassandraColumnSchema;
  private Boolean processWritetime;
  private final ValueProvider<Boolean> setZeroTimestamp;
  private Long backfillTimestamp;

  // Simplified function overload for testing purposes.
  public static BeamRowToBigtableFn create(
      ValueProvider<String> defaultRowKeySeparator, ValueProvider<String> defaultColumnFamily) {
    return new BeamRowToBigtableFn(
        defaultRowKeySeparator,
        defaultColumnFamily,
        StaticValueProvider.of(DEFAULT_SPLIT_LARGE_ROWS),
        MAX_MUTATION_PER_REQUEST,
        null,
        StaticValueProvider.of(true));
  }

  public static BeamRowToBigtableFn createWithSplitLargeRows(
      ValueProvider<String> defaultRowKeySeparator,
      ValueProvider<String> defaultColumnFamily,
      ValueProvider<Boolean> splitLargeRowsFlag,
      int maxMutationsPerRow,
      ValueProvider<String> cassandraColumnSchema,
      ValueProvider<Boolean> setZeroTimestamp) {
    return new BeamRowToBigtableFn(
        defaultRowKeySeparator,
        defaultColumnFamily,
        splitLargeRowsFlag,
        maxMutationsPerRow,
        cassandraColumnSchema,
        setZeroTimestamp);
  }

  /**
   * @param defaultRowKeySeparator the row key field separator. See above for details.
   * @param defaultColumnFamily the default column family to write cell values & column qualifiers.
   */
  private BeamRowToBigtableFn(
      ValueProvider<String> defaultRowKeySeparator,
      ValueProvider<String> defaultColumnFamily,
      ValueProvider<Boolean> splitLargeRowsFlag,
      int maxMutationsPerRequest,
      ValueProvider<String> cassandraColumnSchema,
      ValueProvider<Boolean> setZeroTimestamp) {
    this.defaultColumnFamily = defaultColumnFamily;
    this.defaultRowKeySeparator = defaultRowKeySeparator;
    this.splitLargeRowsFlag = splitLargeRowsFlag;
    this.maxMutationsPerRequest = maxMutationsPerRequest;
    this.cassandraColumnSchema = cassandraColumnSchema;
    this.setZeroTimestamp = setZeroTimestamp;
  }

  @Setup
  public void setup() {
    if (splitLargeRowsFlag != null) {
      splitLargeRows = splitLargeRowsFlag.get();
    }
    splitLargeRows = MoreObjects.firstNonNull(splitLargeRows, DEFAULT_SPLIT_LARGE_ROWS);
    // Process writetime is cassandra column schema is present and accessible to the pipeline.
    processWritetime = cassandraColumnSchema != null && cassandraColumnSchema.isAccessible();
    if (setZeroTimestamp != null && Boolean.TRUE.equals(setZeroTimestamp.get())) {
      backfillTimestamp = 0L;
    } else {
      // If setZeroTimestamp is not present or false, then set backfill timestamp to now in
      // microsecond format.
      backfillTimestamp = Instant.now().toEpochMilli() * 1000;
    }
    LOG.info("splitLargeRows set to: " + splitLargeRows);
    LOG.info("processWritetime set to: " + processWritetime);
    LOG.info("backfillTimestamp set to: " + backfillTimestamp);
  }

  @ProcessElement
  @SuppressWarnings("unused")
  public void processElement(
      @Element Row row, OutputReceiver<KV<ByteString, Iterable<Mutation>>> out) {

    // Generate the Bigtable Rowkey. This key will be used for all Cells for this row.
    ByteString rowkey = generateRowKey(row);

    // Number of mutations accumulated to be outputted.
    int cellsProcessed = 0;

    // Retrieve all fields that are not part of the rowkey. All fields that are part of the
    // rowkey must have a metadata key set to key_order. All other fields should become their own
    // cells in Bigtable.
    List<Field> nonKeyColumns =
        row.getSchema().getFields().stream()
            .filter(
                f ->
                    f.getType()
                        .getMetadataString(CassandraRowMapperFn.KEY_ORDER_METADATA_KEY)
                        .isEmpty())
            .collect(Collectors.toList());

    // Initialize hashmap of column name to int64 timestamps.
    HashMap<String, Long> columnTimestamps = new HashMap<>();
    if (processWritetime) {
      columnTimestamps = getColumnWritetimes(row, nonKeyColumns);
    }

    // Iterate over all the fields with three cases. Primitives, collections and maps.
    List<Mutation> mutationsToAdd = new ArrayList<>();
    for (Field field : nonKeyColumns) {
      TypeName type = field.getType().getTypeName();
      // Get writetime for column if it exists.
      Long timestamp =
          columnTimestamps.containsKey(field.getName())
              ? columnTimestamps.get(field.getName())
              : backfillTimestamp;

      if (processWritetime && field.getName().contains(WRITETIME_COLUMN)) {
        // Skip writetime fields if processWritetime is on.
      } else if (type.isPrimitiveType()) {
        ByteString value = primitiveFieldToBytes(type, row.getValue(field.getName()));
        SetCell cell = createCell(defaultColumnFamily.get(), field.getName(), value, timestamp);
        mutationsToAdd.addAll(Arrays.asList(Mutation.newBuilder().setSetCell(cell).build()));
      } else if (type.isCollectionType()) {
        mutationsToAdd.addAll(createCollectionMutations(row, field, timestamp));
      } else if (type.isMapType()) {
        mutationsToAdd.addAll(createMapMutations(row, field, timestamp));
      } else {
        throw new UnsupportedOperationException(
            "Mapper does not support type:" + field.getType().getTypeName().toString());
      }
    }

    // DoFn return value.
    ImmutableList.Builder<Mutation> mutations = ImmutableList.builder();
    // Iterate over constructed mutations and build them. Split them up if the split large rows flag
    // is on.
    for (Mutation mutationToAdd : mutationsToAdd) {
      mutations.add(mutationToAdd);
      cellsProcessed++;
      if (this.splitLargeRows && cellsProcessed % maxMutationsPerRequest == 0) {
        // Send a MutateRow request when we have accumulated max mutations per request.
        out.output(KV.of(rowkey, mutations.build()));
        mutations = ImmutableList.builder();
      }
    }
    // Flush any remaining mutations.
    ImmutableList remainingMutations = mutations.build();
    if (!remainingMutations.isEmpty()) {
      out.output(KV.of(rowkey, remainingMutations));
    }
  }

  private HashMap<String, Long> getColumnWritetimes(Row row, List<Field> nonKeyColumns) {
    // Writetime values will be joined to the other mutations as timestamps.
    HashMap<String, Long> columnTimestamps = new HashMap<>();
    Pattern writetimeColumn = Pattern.compile(String.format("%s\\((.*)\\)", WRITETIME_COLUMN));
    for (Field field : nonKeyColumns) {
      if (field.getName().contains(WRITETIME_COLUMN)) {
        // Get column name of writetime column, e.g. "writetime(column1)" will be the writetime for
        // "column1".
        Matcher matcher = writetimeColumn.matcher(field.getName());
        matcher.matches();
        String columnName = matcher.group(1);

        // Get timestamp and normalize as necessary.
        Long rawTimestamp = (Long) row.getValue(field.getName());
        Long normalizedTimestamp = normalizeToBigtableTimestamp(rawTimestamp);

        columnTimestamps.put(columnName, normalizedTimestamp);
      }
    }
    return columnTimestamps;
  }

  /**
   * Bigtable requires timestamps to be a microsecond value in millisecond precision. Cassandra
   * timestamps can either be microsecond or millisecond precision. Therefore, this function does
   * two things: 1. casts microsecond timestamps to millisecond precision by setting last 3 digits
   * to 0. 2. casts millisecond timestamps to microsecond value by padding 3 more 0s to the end.
   *
   * @param timestamp in either micro or millisecond format
   * @return microsecond value timestamp in millisecond precision
   */
  Long normalizeToBigtableTimestamp(Long timestamp) {
    if (timestamp < 0L) {
      throw new IllegalArgumentException("Timestamp " + timestamp + " is smaller than 0.");
    }
    Long newTimestamp = timestamp;
    Long nowInMillis = Instant.now().toEpochMilli();
    Long nowInMicros = nowInMillis * 1000;

    if (timestamp <= nowInMillis) {
      // Timestamp is in millisecond format. If so, pad 3 more 0s to the end.
      newTimestamp *= 1000;
    } else if (timestamp <= nowInMicros) {
      // Time is in microsecond format. If so, set last 3 digits to 0.
      newTimestamp /= 1000;
      newTimestamp *= 1000;
    } else {
      // In this case, the timestamp is set in the future. It is not recommended to do this due to
      // implications around Bigtable garbage collection. However, there is nothing wrong with this
      // technically so this action is not blocked.
      LOG.warn(
          String.format(
              "Timestamp set into the future, current time in micros: %s, timestamp: %s",
              nowInMicros, timestamp));
    }
    return newTimestamp;
  }

  /**
   * Method generates a set of Bigtable mutations from the field specified in the input. Example: a
   * collection named ‘mycolumn’ with three values would be expanded into three column qualifiers in
   * inside Cloud Bigtable called, ‘mycolumn[0]’, ‘mycolumn[1]’ ,‘mycolumn[2]’. Cell values will be
   * serialized with the {@link BeamRowToBigtableFn#primitiveFieldToBytes(TypeName, Object)}
   * primitiveFieldToBytes} method.
   *
   * @param row The row to get the collection from.
   * @param field The field pointing to a collection in the row.
   * @param timestamp epoch long timestamp for collection mutations.
   * @return A set of mutation on the format specified above.
   */
  private List<Mutation> createCollectionMutations(Row row, Field field, Long timestamp) {
    List<Mutation> mutations = new ArrayList<>();
    List<Object> list = new ArrayList<Object>(row.getArray(field.getName()));
    TypeName collectionElementType = field.getType().getCollectionElementType().getTypeName();
    for (int i = 0; i < list.size(); i++) {
      String fieldName = field.getName() + "[" + i + "]";
      ByteString value = primitiveFieldToBytes(collectionElementType, list.get(i));
      SetCell cell = createCell(defaultColumnFamily.get(), fieldName, value, timestamp);
      mutations.add(Mutation.newBuilder().setSetCell(cell).build());
    }
    return mutations;
  }

  /**
   * Maps are serialized similarly to {@link BeamRowToBigtableFn#createMapMutations(Row, Field,
   * Long)} collections} however with an additional suffix to denote if the cell is a key or a
   * value. As such a map column in Cassandra named ‘mycolumn’ with two key-value pairs would be
   * expanded into the following column qualifiers: ‘mycolumn[0].key’, ‘mycolumn[1].key’,
   * ‘mycolumn[0].value’, ‘mycolumn[1].value’. Cell values will be serialized with the {@link
   * BeamRowToBigtableFn#primitiveFieldToBytes(TypeName, Object)} primitiveFieldToBytes} method.
   *
   * @param row The row to get the collection from.
   * @param field The field pointing to a collection in the row.
   * @param timestamp epoch long timestamp for map mutations.
   * @return A set of mutation on the format specified above.
   */
  private List<Mutation> createMapMutations(Row row, Field field, Long timestamp) {
    List<Mutation> mutations = new ArrayList<>();
    // We use tree-map here to make sure that the keys are sorted.
    // Otherwise we get unpredictable serialization order of the keys in the mutation.
    Map<Object, Object> map = new TreeMap(row.getMap(field.getName()));
    TypeName mapKeyType = field.getType().getMapKeyType().getTypeName();
    TypeName mapValueType = field.getType().getMapValueType().getTypeName();
    Set<Map.Entry<Object, Object>> entries = map.entrySet();
    Iterator<Map.Entry<Object, Object>> iterator = entries.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Map.Entry<Object, Object> entry = iterator.next();
      // Key
      String keyFieldName = field.getName() + "[" + i + "].key";
      ByteString keyValue = primitiveFieldToBytes(mapKeyType, entry.getKey());
      SetCell keyCell = createCell(defaultColumnFamily.get(), keyFieldName, keyValue, timestamp);
      mutations.add(Mutation.newBuilder().setSetCell(keyCell).build());

      // Value
      String valueFieldName = field.getName() + "[" + i + "].value";
      ByteString valueValue = primitiveFieldToBytes(mapValueType, entry.getValue());
      SetCell valueCell =
          createCell(defaultColumnFamily.get(), valueFieldName, valueValue, timestamp);
      mutations.add(Mutation.newBuilder().setSetCell(valueCell).build());

      i++;
    }
    return mutations;
  }

  /**
   * Helper method to create a SetCell operation.
   *
   * @param columnFamily the column family to apply the value on.
   * @param columnQualifier the column qualifier to apply the value on.
   * @param value the value to apply.
   * @return
   */
  private SetCell createCell(
      String columnFamily, String columnQualifier, ByteString value, Long timestamp) {
    return SetCell.newBuilder()
        .setFamilyName(columnFamily)
        .setColumnQualifier(ByteString.copyFrom(columnQualifier, Charset.defaultCharset()))
        .setValue(value)
        .setTimestampMicros(timestamp)
        .build();
  }

  /**
   * Method serializes a primitive to a ByteString. Most types are serialized with the {@link Bytes
   * toBytes toBytes} method. Bytes and byte arrays pass through as they are whilst DATETIME types
   * gets converted to a ISO8601 formatted String.
   *
   * @param type the {@link Row row} field type
   * @param value the value from the {@link Row row}
   * @return a ByteString.
   */
  private ByteString primitiveFieldToBytes(TypeName type, Object value) {

    if (value == null) {
      return ByteString.EMPTY;
    }

    byte[] bytes;

    switch (type) {
      case BYTE:
        bytes = new byte[1];
        bytes[0] = (Byte) value;
        break;
      case INT16:
        bytes = Bytes.toBytes((Short) value);
        break;
      case INT32:
        bytes = Bytes.toBytes((Integer) value);
        break;
      case INT64:
        bytes = Bytes.toBytes((Long) value);
        break;
      case DECIMAL:
        bytes = Bytes.toBytes((BigDecimal) value);
        break;
      case FLOAT:
        bytes = Bytes.toBytes((Float) value);
        break;
      case DOUBLE:
        bytes = Bytes.toBytes((Double) value);
        break;
      case STRING:
        bytes = Bytes.toBytes((String) value);
        break;
      case DATETIME:
        ReadableInstant dateTime = ((ReadableInstant) value);
        // Write a ISO8601 formatted String.
        bytes = Bytes.toBytes(dateTime.toString());
        break;
      case BOOLEAN:
        bytes = Bytes.toBytes((Boolean) value);
        break;
      case BYTES:
        bytes = (byte[]) value;
        break;
      default:
        throw new UnsupportedOperationException("This method only supports primitives.");
    }

    return ByteString.copyFrom(bytes);
  }

  /**
   * Method serializes a primitive to a String. Most types are serialized with the standard toString
   * method. Bytes and byte arrays pass through as they are whilst DATETIME types gets converted to
   * a ISO8601 formatted String.
   *
   * @param type the {@link Row row} field type
   * @param value the value from the {@link Row row}
   * @return a ByteString.
   */
  protected static String primitiveFieldToString(TypeName type, Object value) {

    if (value == null) {
      return "";
    }

    String string;

    switch (type) {
      case BYTE:
        return BaseEncoding.base64().encode(new byte[] {(byte) value});
      case INT16:
      case DECIMAL:
      case INT64:
      case INT32:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BOOLEAN:
        return value.toString();
      case DATETIME:
        ReadableInstant dateTime = ((ReadableInstant) value);
        // Write a ISO8601 formatted String.
        return dateTime.toString();
      case BYTES:
        return BaseEncoding.base64().encode((byte[]) value);
      default:
        throw new UnsupportedOperationException(
            "Unsupported type: " + type + " This method only supports primitives.");
    }
  }

  /**
   * This method generates a String-based Bigtable Rowkey based on the Beam {@link Row} supplied as
   * input. The method filters out all fields containing a metadata key set to "key_order". The
   * value of this metadata field represents the order in which the field was used in Cassandra when
   * constructing the primary key. Example:
   *
   * <p>If you supply the method with a row having three fields named 'key1', 'key2' and 'cell1'
   * with values 'hello', 'world' and '1'. Where fields 'key1', 'key2' have the metadata
   * "key_order",0 and "key_order",1 and defaultRowKeySeparator '#' the method would return:
   * 'hello#world'
   *
   * @param row the row to create a rowkey from.
   * @return the string formatted rowkey.
   */
  private ByteString generateRowKey(Row row) {
    Map<String, String> keyColumns =
        row.getSchema().getFields().stream()
            .filter(
                f ->
                    !f.getType()
                        .getMetadataString(CassandraRowMapperFn.KEY_ORDER_METADATA_KEY)
                        .isEmpty())
            .collect(
                Collectors.toMap(
                    f -> f.getType().getMetadataString(CassandraRowMapperFn.KEY_ORDER_METADATA_KEY),
                    Field::getName,
                    (f, s) -> f,
                    TreeMap::new));

    // Join all values (field names) in order.
    String rowkey =
        keyColumns.entrySet().stream()
            .map(
                e ->
                    primitiveFieldToString(
                        row.getSchema().getField(e.getValue()).getType().getTypeName(),
                        row.getValue(e.getValue())))
            .collect(Collectors.joining(defaultRowKeySeparator.get()));
    byte[] bytes = Bytes.toBytes(rowkey);
    return ByteString.copyFrom(bytes);
  }
}
