/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.api.client.util.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.ProgressIndicator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ByteBufferWriter;
import org.apache.druid.segment.data.ColumnarDoublesSerializer;
import org.apache.druid.segment.data.ColumnarLongsSerializer;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.data.SingleValueColumnarIntsSerializer;
import org.apache.druid.segment.data.VSizeColumnarIntsSerializer;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public class NestedDataColumnSerializer implements GenericColumnSerializer<StructuredData>
{
  private static final Logger log = new Logger(NestedDataColumnSerializer.class);
  public static final IntTypeStrategy INT_TYPE_STRATEGY = new IntTypeStrategy();

  private final String name;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final IndexSpec indexSpec;
  private final Closer closer;

  private byte[] metadataBytes;
  private GlobalDictionaryIdLookup globalDictionaryIdLookup;
  private SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> fields;
  private GenericIndexedWriter<String> fieldsWriter;
  private NestedLiteralTypeInfo.Writer fieldsInfoWriter;
  private GenericIndexedWriter<String> dictionaryWriter;
  private FixedIndexedWriter<Long> longDictionaryWriter;
  private FixedIndexedWriter<Double> doubleDictionaryWriter;
  private GenericIndexedWriter<StructuredData> rawWriter;
  private ByteBufferWriter<ImmutableBitmap> nullBitmapWriter;
  private MutableBitmap nullRowsBitmap;


  private Map<String, GlobalDictionaryEncodedFieldColumnWriter<?>> fieldWriters;

  private int rowCount = 0;
  private boolean closedForWrite = false;
  private final StructuredDataProcessor fieldProcessor = new StructuredDataProcessor()
  {
    @Override
    public void processLiteralField(String fieldName, Object fieldValue)
    {
      final GlobalDictionaryEncodedFieldColumnWriter<?> writer = fieldWriters.get(fieldName);
      try {
        ExprEval<?> eval = ExprEval.bestEffortOf(fieldValue);
        writer.addValue(eval.value());
      }
      catch (IOException e) {
        throw new RuntimeException(":(");
      }
    }
  };

  public NestedDataColumnSerializer(
      String name,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ProgressIndicator progressIndicator,
      Closer closer
  )
  {
    this.name = name;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.indexSpec = indexSpec;
    this.closer = closer;
    this.globalDictionaryIdLookup = new GlobalDictionaryIdLookup();
  }

  @Override
  public void open() throws IOException
  {
    fieldsWriter = createGenericIndexedWriter(GenericIndexed.STRING_STRATEGY, segmentWriteOutMedium);
    fieldsInfoWriter = new NestedLiteralTypeInfo.Writer(segmentWriteOutMedium);
    fieldsInfoWriter.open();
    dictionaryWriter = createGenericIndexedWriter(GenericIndexed.STRING_STRATEGY, segmentWriteOutMedium);
    longDictionaryWriter = new FixedIndexedWriter<>(
        segmentWriteOutMedium,
        ColumnType.LONG.getStrategy(),
        ByteOrder.nativeOrder(),
        Long.BYTES,
        true
    );
    longDictionaryWriter.open();
    doubleDictionaryWriter = new FixedIndexedWriter<>(
        segmentWriteOutMedium,
        ColumnType.DOUBLE.getStrategy(),
        ByteOrder.nativeOrder(),
        Double.BYTES,
        true
    );
    doubleDictionaryWriter.open();
    rawWriter = createGenericIndexedWriter(
        NestedDataComplexTypeSerde.INSTANCE.getObjectStrategy(),
        segmentWriteOutMedium
    );
    nullBitmapWriter = new ByteBufferWriter<>(
        segmentWriteOutMedium,
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    nullBitmapWriter.open();
    nullRowsBitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
  }

  public void serializeFields(SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> fields) throws IOException
  {
    this.fields = fields;
    this.fieldWriters = Maps.newHashMapWithExpectedSize(fields.size());
    for (Map.Entry<String, NestedLiteralTypeInfo.MutableTypeSet> field : fields.entrySet()) {
      fieldsWriter.write(field.getKey());
      fieldsInfoWriter.write(field.getValue());
      final GlobalDictionaryEncodedFieldColumnWriter writer;
      ColumnType type = field.getValue().getSingleType();
      if (type != null) {
        if (Types.is(type, ValueType.STRING)) {
          writer = new StringFieldColumnWriter();
        } else if (Types.is(type, ValueType.LONG)) {
          writer = new LongFieldColumnWriter();
        } else {
          writer = new DoubleFieldColumnWriter();
        }
      } else {
        writer = new AnyFieldColumnWriter();
      }
      writer.open();
      fieldWriters.put(field.getKey(), writer);
    }
  }

  protected void serializeStringDictionary(Iterable<String> dictionaryValues) throws IOException
  {
    dictionaryWriter.write(null);
    globalDictionaryIdLookup.addString(null);
    for (String value : dictionaryValues) {
      if (NullHandling.emptyToNullIfNeeded(value) == null) {
        continue;
      }
      dictionaryWriter.write(value);
      value = NullHandling.emptyToNullIfNeeded(value);
      globalDictionaryIdLookup.addString(value);
    }
  }

  protected void serializeLongDictionary(Iterable<Long> dictionaryValues) throws IOException
  {
    for (Long value : dictionaryValues) {
      if (value == null) {
        continue;
      }
      longDictionaryWriter.write(value);
      globalDictionaryIdLookup.addLong(value);
    }
  }

  protected void serializeDoubleDictionary(Iterable<Double> dictionaryValues) throws IOException
  {
    for (Double value : dictionaryValues) {
      if (value == null) {
        continue;
      }
      doubleDictionaryWriter.write(value);
      globalDictionaryIdLookup.addDouble(value);
    }
  }

  @Override
  public void serialize(ColumnValueSelector<? extends StructuredData> selector) throws IOException
  {
    StructuredData data = selector.getObject();
    if (data == null) {
      nullRowsBitmap.add(rowCount);
    }
    rawWriter.write(data);
    if (data != null) {
      List<String> processed = fieldProcessor.processFields(data.getValue());
      Set<String> set = ImmutableSet.copyOf(processed);
      Set<String> missing = new HashSet<>();
      for (String field : fields.keySet()) {
        if (!set.contains(field)) {
          missing.add(field);
        }
      }

      for (String field : missing) {
        fieldWriters.get(field).addValue(null);
      }
    } else {
      for (String field : fields.keySet()) {
        fieldWriters.get(field).addValue(null);
      }
    }
    rowCount++;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    if (!closedForWrite) {
      closedForWrite = true;
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      IndexMerger.SERIALIZER_UTILS.writeString(
          baos,
          NestedDataComplexTypeSerde.OBJECT_MAPPER.writeValueAsString(
              new NestedDataColumnMetadata(
                  ByteOrder.nativeOrder(),
                  indexSpec.getBitmapSerdeFactory(),
                  name,
                  !nullRowsBitmap.isEmpty()
              )
          )
      );
      this.metadataBytes = baos.toByteArray();
      this.nullBitmapWriter.write(nullRowsBitmap);
    }

    long size = 1; // flag if version >= compressed
    size += metadataBytes.length;
    if (fieldsWriter != null) {
      size += fieldsWriter.getSerializedSize();
    }
    if (fieldsInfoWriter != null) {
      size += fieldsInfoWriter.getSerializedSize();
    }
    if (dictionaryWriter != null) {
      size += dictionaryWriter.getSerializedSize();
    }
    if (longDictionaryWriter != null) {
      size += longDictionaryWriter.getSerializedSize();
    }
    if (doubleDictionaryWriter != null) {
      size += doubleDictionaryWriter.getSerializedSize();
    }
    if (rawWriter != null) {
      size += rawWriter.getSerializedSize();
    }
    if (nullBitmapWriter != null && !nullRowsBitmap.isEmpty()) {
      size += nullBitmapWriter.getSerializedSize();
    }
    return size;
  }

  @Override
  public void writeTo(
      WritableByteChannel channel,
      FileSmoosher smoosher
  ) throws IOException
  {
    Preconditions.checkState(closedForWrite, "Not closed yet!");

    // version 1
    channel.write(ByteBuffer.wrap(new byte[]{0x01}));
    channel.write(ByteBuffer.wrap(metadataBytes));
    fieldsWriter.writeTo(channel, smoosher);
    fieldsInfoWriter.writeTo(channel, smoosher);
    dictionaryWriter.writeTo(channel, smoosher);
    longDictionaryWriter.writeTo(channel, smoosher);
    doubleDictionaryWriter.writeTo(channel, smoosher);

    rawWriter.writeTo(channel, smoosher);
    if (!nullRowsBitmap.isEmpty()) {
      nullBitmapWriter.writeTo(channel, smoosher);
    }

    // close the SmooshedWriter since we are done here, so we don't write to a temporary file per sub-column
    // In the future, it would be best if the writeTo() itself didn't take a channel but was expected to actually
    // open its own channels on the FileSmoosher object itself.  Or some other thing that give this Serializer
    // total control over when resources are opened up and when they are closed.  Until then, we are stuck
    // with a very tight coupling of this code with how the external "driver" is working.
    if (channel instanceof SmooshedWriter) {
      channel.close();
    }
    for (Map.Entry<String, NestedLiteralTypeInfo.MutableTypeSet> field : fields.entrySet()) {
      fieldWriters.get(field.getKey()).writeTo(field.getKey(), smoosher);
    }
    log.info("Column [%s] serialized successfully with [%d] nested columns.", name, fields.size());
  }

  private <T> GenericIndexedWriter<T> createGenericIndexedWriter(
      ObjectStrategy<T> objectStrategy,
      SegmentWriteOutMedium writeOutMedium
  ) throws IOException
  {
    GenericIndexedWriter<T> writer = new GenericIndexedWriter<>(writeOutMedium, name, objectStrategy);
    writer.open();
    return writer;
  }

  private <T> GenericIndexedWriter<T> createUnsortedGenericIndexedWriter(
      ObjectStrategy<T> objectStrategy,
      SegmentWriteOutMedium writeOutMedium
  ) throws IOException
  {
    GenericIndexedWriter<T> writer = createGenericIndexedWriter(objectStrategy, writeOutMedium);
    writer.setObjectsNotSorted();
    return writer;
  }

  public static String getFieldFileName(String field, String fileNameBase)
  {
    return StringUtils.format("%s_%s", fileNameBase, field);
  }

  abstract class GlobalDictionaryEncodedFieldColumnWriter<T>
  {
    protected final IntSortedSet dictionary = new IntAVLTreeSet();
    protected final Int2ObjectMap<MutableBitmap> bitmaps = new Int2ObjectAVLTreeMap<>();

    protected FixedIndexedWriter<Integer> intermediateValueWriter;
    // maybe someday we allow no bitmap indexes or multi-value columns
    protected int flags = DictionaryEncodedColumnPartSerde.NO_FLAGS;
    protected DictionaryEncodedColumnPartSerde.VERSION version = null;
    protected SingleValueColumnarIntsSerializer encodedValueSerializer;

    T processValue(Object value)
    {
      return (T) value;
    }
    abstract int lookupId(T value);

    abstract void writeColumnTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException;

    void openColumnSerializer(String field, SegmentWriteOutMedium medium) throws IOException
    {
      if (indexSpec.getDimensionCompression() != CompressionStrategy.UNCOMPRESSED) {
        this.version = DictionaryEncodedColumnPartSerde.VERSION.COMPRESSED;
        encodedValueSerializer = CompressedVSizeColumnarIntsSerializer.create(
            field,
            medium,
            name,
            dictionary.lastInt(),
            indexSpec.getDimensionCompression()
        );
      } else {
        encodedValueSerializer = new VSizeColumnarIntsSerializer(medium, dictionary.lastInt());
        this.version = DictionaryEncodedColumnPartSerde.VERSION.UNCOMPRESSED_SINGLE_VALUE;
      }
      encodedValueSerializer.open();
    }

    void serializeRow(Int2IntMap lookup, int rowId) throws IOException
    {
      encodedValueSerializer.addValue(lookup.get(rowId));
    }

    long getSerializedColumnSize() throws IOException
    {
      return Integer.BYTES + Integer.BYTES + encodedValueSerializer.getSerializedSize();
    }

    public void open() throws IOException
    {
      intermediateValueWriter = new FixedIndexedWriter<>(
          segmentWriteOutMedium,
          INT_TYPE_STRATEGY,
          ByteOrder.nativeOrder(),
          Integer.BYTES,
          false
      );
      intermediateValueWriter.open();
    }

    public void writeLongAndDoubleColumnLength(WritableByteChannel channel, int longLength, int doubleLength)
        throws IOException
    {
      ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());
      intBuffer.position(0);
      intBuffer.putInt(longLength);
      intBuffer.flip();
      Channels.writeFully(channel, intBuffer);
      intBuffer.position(0);
      intBuffer.limit(intBuffer.capacity());
      intBuffer.putInt(doubleLength);
      intBuffer.flip();
      Channels.writeFully(channel, intBuffer);
    }

    public void addValue(Object val) throws IOException
    {
      final T value = processValue(val);
      final int id = lookupId(value);
      dictionary.add(id);

      MutableBitmap bitmap = bitmaps.get(id);
      if (bitmap == null) {
        bitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
        bitmaps.put(id, bitmap);
      }
      bitmap.add(rowCount);

      intermediateValueWriter.write(id);
    }

    public void writeTo(String field, FileSmoosher smoosher) throws IOException
    {
      final SegmentWriteOutMedium tmpWriteoutMedium = segmentWriteOutMedium.makeChildWriteOutMedium();
      // create dictionary writer
      final FixedIndexedWriter<Integer> sortedDictionaryWriter = new FixedIndexedWriter<>(
          tmpWriteoutMedium,
          INT_TYPE_STRATEGY,
          ByteOrder.nativeOrder(),
          Integer.BYTES,
          true
      );
      sortedDictionaryWriter.open();
      for (int s : dictionary) {
        sortedDictionaryWriter.write(s);
      }

      openColumnSerializer(field, tmpWriteoutMedium);

      Int2IntMap dictionaryLookup = new Int2IntArrayMap(dictionary.size());
      int counter = 0;
      for (int i : dictionary) {
        dictionaryLookup.put(i, counter++);
      }
      for (int i = 0; i < rowCount; i++) {
        int id = intermediateValueWriter.get(i);
        serializeRow(dictionaryLookup, id);
      }

      // create immutable bitmaps, write in same order as dictionary
      final GenericIndexedWriter<ImmutableBitmap> bitmapsWriter = createUnsortedGenericIndexedWriter(
          indexSpec.getBitmapSerdeFactory().getObjectStrategy(),
          tmpWriteoutMedium
      );
      for (int value : dictionary) {
        bitmapsWriter.write(indexSpec.getBitmapSerdeFactory()
                                     .getBitmapFactory()
                                     .makeImmutableBitmap(bitmaps.get(value)));
      }

      final Serializer fieldSerializer = new Serializer()
      {
        @Override
        public long getSerializedSize() throws IOException
        {
          return 1 + Integer.BYTES +
                 sortedDictionaryWriter.getSerializedSize() +
                 bitmapsWriter.getSerializedSize() +
                 getSerializedColumnSize();
        }

        @Override
        public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
        {
          Channels.writeFully(channel, ByteBuffer.wrap(new byte[]{version.asByte()}));
          channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));
          sortedDictionaryWriter.writeTo(channel, smoosher);
          writeColumnTo(channel, smoosher);
          bitmapsWriter.writeTo(channel, smoosher);
        }
      };
      final String fieldName = getFieldFileName(field, name);
      final long size = fieldSerializer.getSerializedSize();
      log.debug("Column [%s] serializing [%s] field of size [%d].", name, field, size);
      try (SmooshedWriter smooshChannel = smoosher.addWithSmooshedWriter(fieldName, size)) {
        fieldSerializer.writeTo(smooshChannel, smoosher);
      }
      finally {
        tmpWriteoutMedium.close();
      }
    }
  }

  private class StringFieldColumnWriter extends GlobalDictionaryEncodedFieldColumnWriter<String>
  {
    @Override
    String processValue(Object value)
    {
      return String.valueOf(value);
    }

    @Override
    int lookupId(String value)
    {
      return globalDictionaryIdLookup.lookupString(value);
    }

    @Override
    void writeColumnTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
    {
      writeLongAndDoubleColumnLength(channel, 0, 0);
      encodedValueSerializer.writeTo(channel, smoosher);
    }
  }

  private class LongFieldColumnWriter extends GlobalDictionaryEncodedFieldColumnWriter<Long>
  {
    private ColumnarLongsSerializer longsSerializer;

    @Override
    int lookupId(Long value)
    {
      return globalDictionaryIdLookup.lookupLong(value);
    }

    @Override
    void openColumnSerializer(String field, SegmentWriteOutMedium medium) throws IOException
    {
      super.openColumnSerializer(field, medium);
      longsSerializer = CompressionFactory.getLongSerializer(
          field,
          medium,
          StringUtils.format("%s.long_column", name),
          ByteOrder.nativeOrder(),
          indexSpec.getLongEncoding(),
          indexSpec.getDimensionCompression()
      );
      longsSerializer.open();
    }

    @Override
    void serializeRow(Int2IntMap lookup, int rowId) throws IOException
    {
      super.serializeRow(lookup, rowId);
      Long l = globalDictionaryIdLookup.lookupLong(rowId);
      if (l == null) {
        longsSerializer.add(0);
      } else {
        longsSerializer.add(l);
      }
    }

    @Override
    void writeColumnTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
    {
      writeLongAndDoubleColumnLength(channel, Ints.checkedCast(longsSerializer.getSerializedSize()), 0);
      longsSerializer.writeTo(channel, smoosher);
      encodedValueSerializer.writeTo(channel, smoosher);
    }

    @Override
    long getSerializedColumnSize() throws IOException
    {
      return super.getSerializedColumnSize() + longsSerializer.getSerializedSize();
    }
  }

  private class DoubleFieldColumnWriter extends GlobalDictionaryEncodedFieldColumnWriter<Double>
  {
    private ColumnarDoublesSerializer doublesSerializer;

    @Override
    int lookupId(Double value)
    {
      return globalDictionaryIdLookup.lookupDouble(value);
    }

    @Override
    void openColumnSerializer(String field, SegmentWriteOutMedium medium) throws IOException
    {
      super.openColumnSerializer(field, medium);
      doublesSerializer = CompressionFactory.getDoubleSerializer(
          field,
          medium,
          StringUtils.format("%s.double_column", name),
          ByteOrder.nativeOrder(),
          indexSpec.getDimensionCompression()
      );
      doublesSerializer.open();
    }

    @Override
    void serializeRow(Int2IntMap lookup, int rowId) throws IOException
    {
      super.serializeRow(lookup, rowId);
      Double d = globalDictionaryIdLookup.lookupDouble(rowId);
      if (d == null) {
        doublesSerializer.add(0.0);
      } else {
        doublesSerializer.add(d);
      }
    }

    @Override
    void writeColumnTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
    {
      writeLongAndDoubleColumnLength(channel, 0, Ints.checkedCast(doublesSerializer.getSerializedSize()));
      doublesSerializer.writeTo(channel, smoosher);
      encodedValueSerializer.writeTo(channel, smoosher);
    }

    @Override
    long getSerializedColumnSize() throws IOException
    {
      return super.getSerializedColumnSize() + doublesSerializer.getSerializedSize();
    }
  }

  private class AnyFieldColumnWriter extends GlobalDictionaryEncodedFieldColumnWriter<Object>
  {
    @Override
    int lookupId(Object value)
    {
      if (value == null) {
        return 0;
      }
      if (value instanceof Long) {
        return globalDictionaryIdLookup.lookupLong((Long) value);
      } else if (value instanceof Double) {
        return globalDictionaryIdLookup.lookupDouble((Double) value);
      } else {
        return globalDictionaryIdLookup.lookupString((String) value);
      }
    }

    @Override
    void writeColumnTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
    {
      writeLongAndDoubleColumnLength(channel, 0, 0);
      encodedValueSerializer.writeTo(channel, smoosher);
    }
  }

  private static final class IntTypeStrategy implements TypeStrategy<Integer>
  {
    @Override
    public int estimateSizeBytes(Integer value)
    {
      return Integer.BYTES;
    }

    @Override
    public Integer read(ByteBuffer buffer)
    {
      return buffer.getInt();
    }

    @Override
    public int write(ByteBuffer buffer, Integer value, int maxSizeBytes)
    {
      TypeStrategies.checkMaxSize(buffer.remaining(), maxSizeBytes, ColumnType.LONG);
      final int sizeBytes = Integer.BYTES;
      final int remaining = maxSizeBytes - sizeBytes;
      if (remaining >= 0) {
        buffer.putInt(value);
        return sizeBytes;
      }
      return remaining;
    }

    @Override
    public int compare(Integer o1, Integer o2)
    {
      return Integer.compare(o1, o2);
    }
  }
}
