/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.samples.caffeine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.imply.druid.ingest.samples.SampleStore;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.druid.client.indexing.SamplerResponse;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class CaffeineSampleStore implements SampleStore
{
  private static final int FIXED_COST = 8; // Minimum cost in "weight" per entry;
  private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final LZ4FastDecompressor LZ4_DECOMPRESSOR = LZ4_FACTORY.fastDecompressor();
  private static final LZ4Compressor LZ4_COMPRESSOR = LZ4_FACTORY.fastCompressor();

  private final Cache<String, byte[]> cache;
  private final ObjectMapper jsonMapper;

  public CaffeineSampleStore(CaffeineSampleStoreConfig config, ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
    this.cache = Caffeine.newBuilder()
                         .expireAfterWrite(config.getCacheExpireTimeMinutes(), TimeUnit.MINUTES)
                         .maximumWeight(config.getMaxSizeBytes())
                         .weigher((String jobId, byte[] bytes) -> FIXED_COST + bytes.length)
                         .build();
  }

  @Nullable
  @Override
  public SamplerResponse getSamplerResponse(String jobId) throws IOException
  {
    byte[] bytes = cache.getIfPresent(jobId);
    if (bytes == null) {
      return null;
    }
    return jsonMapper.readValue(decompress(bytes), SamplerResponse.class);
  }

  @Override
  public void storeSample(String jobId, SamplerResponse samplerResponse)
  {
    try {
      cache.put(jobId, compress(jsonMapper.writeValueAsBytes(samplerResponse)));
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteSample(String jobId)
  {
    cache.invalidate(jobId);
  }

  /**
   * copied from {@link org.apache.druid.client.cache.CaffeineCache}
   */
  private byte[] decompress(byte[] bytes)
  {
    if (bytes == null) {
      return null;
    }
    final int decompressedLen = ByteBuffer.wrap(bytes).getInt();
    final byte[] out = new byte[decompressedLen];
    LZ4_DECOMPRESSOR.decompress(bytes, Integer.BYTES, out, 0, out.length);
    return out;
  }

  /**
   * copied from {@link org.apache.druid.client.cache.CaffeineCache}
   */
  private byte[] compress(byte[] value)
  {
    final int len = LZ4_COMPRESSOR.maxCompressedLength(value.length);
    final byte[] out = new byte[len];
    final int compressedSize = LZ4_COMPRESSOR.compress(value, 0, value.length, out, 0);
    return ByteBuffer.allocate(compressedSize + Integer.BYTES)
                     .putInt(value.length)
                     .put(out, 0, compressedSize)
                     .array();
  }
}
