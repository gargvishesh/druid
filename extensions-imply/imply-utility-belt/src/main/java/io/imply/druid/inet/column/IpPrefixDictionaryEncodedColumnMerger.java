/*
 *
 *  * Copyright (c) Imply Data, Inc. All rights reserved.
 *  *
 *  * This software is the confidential and proprietary information
 *  * of Imply Data, Inc. You shall not disclose such Confidential
 *  * Information and shall use it only in accordance with the terms
 *  * of the license agreement you entered into with Imply.
 *
 *
 */

package io.imply.druid.inet.column;

import com.google.common.collect.PeekingIterator;
import io.imply.druid.inet.IpAddressModule;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.DictionaryEncodedColumnMerger;
import org.apache.druid.segment.DictionaryMergingIterator;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.ProgressIndicator;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSerde;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Comparator;

public class IpPrefixDictionaryEncodedColumnMerger extends DictionaryEncodedColumnMerger<IpPrefixBlob>
{
  public static final SerializerUtils SERIALIZER_UTILS = new SerializerUtils();
  public static final Comparator<Pair<Integer, PeekingIterator<IpPrefixBlob>>> DICTIONARY_MERGING_COMPARATOR =
      DictionaryMergingIterator.makePeekingComparator();

  private static final Indexed<IpPrefixBlob> NULL_VALUE = new ListIndexed<>(Collections.singletonList(null));
  private static final byte VERSION = 0x00;

  private final byte[] metadataBytes;

  public IpPrefixDictionaryEncodedColumnMerger(
      String name,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilites,
      ProgressIndicator progressIndicator,
      Closer closer
  )
  {
    super(name, indexSpec, segmentWriteOutMedium, capabilites, progressIndicator, closer);
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      SERIALIZER_UTILS.writeString(
          baos,
          IpPrefixComplexTypeSerde.JSON_MAPPER.writeValueAsString(
              new IpAddressBlobColumnMetadata(indexSpec.getBitmapSerdeFactory())
          )
      );
      this.metadataBytes = baos.toByteArray();
    }
    catch (IOException e) {
      throw new RuntimeException("wat");
    }
  }

  @Override
  protected Comparator<Pair<Integer, PeekingIterator<IpPrefixBlob>>> getDictionaryMergingComparator()
  {
    return DICTIONARY_MERGING_COMPARATOR;
  }

  @Override
  protected Indexed<IpPrefixBlob> getNullDimValue()
  {
    return NULL_VALUE;
  }

  @Override
  protected ObjectStrategy<IpPrefixBlob> getObjectStrategy()
  {
    return IpPrefixBlob.STRATEGY;
  }

  @Nullable
  @Override
  protected IpPrefixBlob coerceValue(IpPrefixBlob value)
  {
    return value;
  }

  @Override
  public ColumnDescriptor makeColumnDescriptor()
  {
    return new ColumnDescriptor.Builder()
        .setValueType(ValueType.COMPLEX)
        .setHasMultipleValues(false)
        .addSerde(
            ComplexColumnPartSerde.serializerBuilder()
                                  .withTypeName(IpAddressModule.PREFIX_TYPE_NAME)
                                  .withDelegate(new Serializer()
                                  {
                                    @Override
                                    public long getSerializedSize() throws IOException
                                    {

                                      // version
                                      long size = 1;

                                      size += metadataBytes.length;
                                      if (dictionaryWriter != null) {
                                        size += dictionaryWriter.getSerializedSize();
                                      }
                                      if (encodedValueSerializer != null) {
                                        size += encodedValueSerializer.getSerializedSize();
                                      }
                                      if (bitmapWriter != null) {
                                        size += bitmapWriter.getSerializedSize();
                                      }
                                      return size;
                                    }

                                    @Override
                                    public void writeTo(
                                        WritableByteChannel channel,
                                        FileSmoosher smoosher
                                    ) throws IOException
                                    {
                                      Channels.writeFully(channel, ByteBuffer.wrap(new byte[]{VERSION}));
                                      channel.write(ByteBuffer.wrap(metadataBytes));
                                      if (dictionaryWriter != null) {
                                        dictionaryWriter.writeTo(channel, smoosher);
                                      }
                                      if (encodedValueSerializer != null) {
                                        encodedValueSerializer.writeTo(channel, smoosher);
                                      }
                                      if (bitmapWriter != null) {
                                        bitmapWriter.writeTo(channel, smoosher);
                                      }
                                    }
                                  }).build()
        )
        .build();

  }
}
