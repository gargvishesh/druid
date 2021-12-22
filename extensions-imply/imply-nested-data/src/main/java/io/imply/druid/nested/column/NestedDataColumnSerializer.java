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
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.ProgressIndicator;
import org.apache.druid.segment.data.ByteBufferWriter;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

public class NestedDataColumnSerializer implements GenericColumnSerializer<StructuredData>
{
  private static final Logger log = new Logger(NestedDataColumnSerializer.class);

  private final String name;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final IndexSpec indexSpec;
  private final Closer closer;

  private byte[] metadataBytes;
  private Object2IntMap<String> lookup;
  private SortedSet<String> fields;
  private GenericIndexedWriter<String> fieldsDictionaryWriter;
  private GenericIndexedWriter<String> dictionaryWriter;
  private GenericIndexedWriter<StructuredData> rawWriter;
  private ByteBufferWriter<ImmutableBitmap> nullBitmapWriter;
  private MutableBitmap nullRowsBitmap;


  private Map<String, StringFieldColumnWriter> fieldWriters;

  private int dictionarySize;
  private int rowCount = 0;
  private boolean closedForWrite = false;
  private final StructuredDataProcessor fieldProcessor = new StructuredDataProcessor()
  {
    @Override
    void processLiteralField(String fieldName, Object fieldValue)
    {
      final StringFieldColumnWriter writer = fieldWriters.get(fieldName);
      try {
        writer.addValue(String.valueOf(fieldValue));
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
    this.lookup = new Object2IntLinkedOpenHashMap<>();
  }

  @Override
  public void open() throws IOException
  {
    fieldsDictionaryWriter = createGenericIndexedWriter(GenericIndexed.STRING_STRATEGY);
    dictionaryWriter = createGenericIndexedWriter(GenericIndexed.STRING_STRATEGY);
    rawWriter = createGenericIndexedWriter(NestedDataComplexTypeSerde.INSTANCE.getObjectStrategy());
    nullBitmapWriter = new ByteBufferWriter<>(
        segmentWriteOutMedium,
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    nullBitmapWriter.open();
    nullRowsBitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
  }

  public void serializeFields(SortedSet<String> fields) throws IOException
  {
    this.fields = fields;
    this.fieldWriters = Maps.newHashMapWithExpectedSize(fields.size());
    for (String field : fields) {
      fieldsDictionaryWriter.write(field);
      final StringFieldColumnWriter writer = new StringFieldColumnWriter();
      writer.open();
      fieldWriters.put(field, writer);
    }
  }

  protected void serializeDictionary(Iterable<String> dictionaryValues) throws IOException
  {
    dictionaryWriter.write(null);
    lookup.put(null, dictionarySize++);
    for (String value : dictionaryValues) {
      if (NullHandling.emptyToNullIfNeeded(value) == null) {
        continue;
      }
      dictionaryWriter.write(value);
      value = NullHandling.emptyToNullIfNeeded(value);
      lookup.put(value, dictionarySize);
      dictionarySize++;
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
      Set<String> missing = Sets.difference(fields, ImmutableSet.copyOf(processed));
      for (String field : missing) {
        fieldWriters.get(field).addValue(null);
      }
    } else {
      for (String field : fields) {
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
      for (String field : fields) {
        fieldWriters.get(field).closeWriter(field);
      }
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
    if (fieldsDictionaryWriter != null) {
      size += fieldsDictionaryWriter.getSerializedSize();
    }
    if (dictionaryWriter != null) {
      size += dictionaryWriter.getSerializedSize();
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

    channel.write(ByteBuffer.wrap(new byte[]{0}));
    channel.write(ByteBuffer.wrap(metadataBytes));
    fieldsDictionaryWriter.writeTo(channel, smoosher);
    dictionaryWriter.writeTo(channel, smoosher);

    rawWriter.writeTo(channel, smoosher);
    if (!nullRowsBitmap.isEmpty()) {

      nullBitmapWriter.writeTo(channel, smoosher);
    }

    for (String field : fields) {
      fieldWriters.get(field).writeTo(field, smoosher);
    }
    log.info("Column [%s] serialized successfully with [%d] nested columns.", name, fields.size());
  }

  private <T> GenericIndexedWriter<T> createGenericIndexedWriter(ObjectStrategy<T> objectStrategy) throws IOException
  {
    GenericIndexedWriter<T> writer = new GenericIndexedWriter<>(segmentWriteOutMedium, name, objectStrategy);
    writer.open();
    return writer;
  }

  private <T> GenericIndexedWriter<T> createUnsortedGenericIndexedWriter(
      ObjectStrategy<T> objectStrategy
  ) throws IOException
  {
    GenericIndexedWriter<T> writer = createGenericIndexedWriter(objectStrategy);
    writer.setObjectsNotSorted();
    return writer;
  }

  public static String getFieldFileName(String field, String fileNameBase)
  {
    return StringUtils.format("%s_%s", fileNameBase, field);
  }

  private class StringFieldColumnWriter
  {
    private final IntSortedSet dictionary = new IntAVLTreeSet();
    private final Int2ObjectMap<MutableBitmap> bitmaps = new Int2ObjectAVLTreeMap<>();
    private final ObjectStrategy<Integer> intStrategy = NestedFieldStringDictionaryEncodedColumn.makeDictionaryStrategy(
        ByteOrder.nativeOrder());

    private GenericIndexedWriter<Integer> intermediateValueWriter;
    private Serializer fieldSerializer;
    // maybe someday we allow no bitmap indexes or multi-value columns
    private int flags = DictionaryEncodedColumnPartSerde.NO_FLAGS;
    private DictionaryEncodedColumnPartSerde.VERSION version = null;

    void open() throws IOException
    {
      intermediateValueWriter = createUnsortedGenericIndexedWriter(intStrategy);
    }

    void addValue(String value) throws IOException
    {
      final int id = lookup.getInt(value);
      dictionary.add(id);

      MutableBitmap bitmap = bitmaps.get(id);
      if (bitmap == null) {
        bitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
        bitmaps.put(id, bitmap);
      }
      bitmap.add(rowCount);

      intermediateValueWriter.write(id);
    }

    void closeWriter(String field) throws IOException
    {
      // create dictionary writer
      final GenericIndexedWriter<Integer> sortedDictionaryWriter = createGenericIndexedWriter(intStrategy);
      for (int s : dictionary) {
        sortedDictionaryWriter.write(s);
      }

      // create values writer
      final SingleValueColumnarIntsSerializer encodedValueSerializer;
      if (indexSpec.getDimensionCompression() != CompressionStrategy.UNCOMPRESSED) {
        this.version = DictionaryEncodedColumnPartSerde.VERSION.COMPRESSED;
        encodedValueSerializer = CompressedVSizeColumnarIntsSerializer.create(
            field,
            segmentWriteOutMedium,
            name,
            lookup.size(),
            indexSpec.getDimensionCompression()
        );
      } else {
        encodedValueSerializer = new VSizeColumnarIntsSerializer(segmentWriteOutMedium, dictionary.size());
        this.version = DictionaryEncodedColumnPartSerde.VERSION.UNCOMPRESSED_SINGLE_VALUE;
      }
      encodedValueSerializer.open();

      Int2IntMap dictionaryLookup = new Int2IntArrayMap(dictionary.size());
      int counter = 0;
      for (int i : dictionary) {
        dictionaryLookup.put(i, counter++);
      }
      for (int i = 0; i < rowCount; i++) {
        encodedValueSerializer.addValue(dictionaryLookup.get((int) intermediateValueWriter.get(i)));
      }

      // create immutable bitmaps, write in same order as dictionary
      final GenericIndexedWriter<ImmutableBitmap> bitmapsWriter = createUnsortedGenericIndexedWriter(indexSpec.getBitmapSerdeFactory()
                                                                                                              .getObjectStrategy());
      for (int value : dictionary) {
        bitmapsWriter.write(indexSpec.getBitmapSerdeFactory()
                                     .getBitmapFactory()
                                     .makeImmutableBitmap(bitmaps.get(value)));
      }

      fieldSerializer = new Serializer()
      {
        @Override
        public long getSerializedSize() throws IOException
        {
          return 1 + Integer.BYTES + sortedDictionaryWriter.getSerializedSize() +
                 encodedValueSerializer.getSerializedSize() +
                 bitmapsWriter.getSerializedSize();
        }

        @Override
        public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
        {
          Channels.writeFully(channel, ByteBuffer.wrap(new byte[]{version.asByte()}));
          channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));
          sortedDictionaryWriter.writeTo(channel, smoosher);
          encodedValueSerializer.writeTo(channel, smoosher);
          bitmapsWriter.writeTo(channel, smoosher);
        }
      };
    }

    void writeTo(String field, FileSmoosher smoosher) throws IOException
    {
      final String fieldName = getFieldFileName(field, name);
      final long size = fieldSerializer.getSerializedSize();
      log.info("Column [%s] serializing [%s] field of size [%d].", name, field, size);
      try (SmooshedWriter smooshChannel = smoosher.addWithSmooshedWriter(fieldName, size)) {
        fieldSerializer.writeTo(smooshChannel, smoosher);
      }
    }
  }
}
