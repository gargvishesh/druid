/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;

import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;

/**
 * See {@link DelegateOrMinKeyCollector} for details.
 */
public class DelegateOrMinKeyCollectorFactory<TDelegate extends KeyCollector<TDelegate>, TSnapshot extends KeyCollectorSnapshot>
    implements KeyCollectorFactory<DelegateOrMinKeyCollector<TDelegate>, DelegateOrMinKeyCollectorSnapshot<TSnapshot>>
{
  private final Comparator<ClusterByKey> comparator;
  private final KeyCollectorFactory<TDelegate, TSnapshot> delegateFactory;

  public DelegateOrMinKeyCollectorFactory(
      final Comparator<ClusterByKey> comparator,
      final KeyCollectorFactory<TDelegate, TSnapshot> delegateFactory
  )
  {
    this.comparator = comparator;
    this.delegateFactory = delegateFactory;
  }

  @Override
  public DelegateOrMinKeyCollector<TDelegate> newKeyCollector()
  {
    return new DelegateOrMinKeyCollector<>(comparator, delegateFactory.newKeyCollector(), null);
  }

  @Override
  public JsonDeserializer<DelegateOrMinKeyCollectorSnapshot<TSnapshot>> snapshotDeserializer()
  {
    final JsonDeserializer<TSnapshot> delegateDeserializer = delegateFactory.snapshotDeserializer();

    return new JsonDeserializer<DelegateOrMinKeyCollectorSnapshot<TSnapshot>>()
    {
      @Override
      public DelegateOrMinKeyCollectorSnapshot<TSnapshot> deserialize(JsonParser jp, DeserializationContext ctxt)
          throws IOException
      {
        TSnapshot delegateSnapshot = null;
        ClusterByKey minKey = null;

        if (!jp.isExpectedStartObjectToken()) {
          ctxt.reportWrongTokenException(this, JsonToken.START_OBJECT, null);
        }

        JsonToken token;

        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
          if (token != JsonToken.FIELD_NAME) {
            ctxt.reportWrongTokenException(this, JsonToken.FIELD_NAME, null);
          }

          final String fieldName = jp.getText();
          jp.nextToken();

          if (DelegateOrMinKeyCollectorSnapshot.FIELD_SNAPSHOT.equals(fieldName)) {
            delegateSnapshot = delegateDeserializer.deserialize(jp, ctxt);
          } else if (DelegateOrMinKeyCollectorSnapshot.FIELD_MIN_KEY.equals(fieldName)) {
            minKey = jp.readValueAs(ClusterByKey.class);
          }
        }

        return new DelegateOrMinKeyCollectorSnapshot<>(delegateSnapshot, minKey);
      }
    };
  }

  @Override
  public DelegateOrMinKeyCollectorSnapshot<TSnapshot> toSnapshot(final DelegateOrMinKeyCollector<TDelegate> collector)
  {
    final ClusterByKey minKeyForSnapshot;

    if (!collector.getDelegate().isPresent() && !collector.isEmpty()) {
      minKeyForSnapshot = collector.minKey();
    } else {
      minKeyForSnapshot = null;
    }

    return new DelegateOrMinKeyCollectorSnapshot<>(
        collector.getDelegate().map(delegateFactory::toSnapshot).orElse(null),
        minKeyForSnapshot
    );
  }

  @Override
  public DelegateOrMinKeyCollector<TDelegate> fromSnapshot(final DelegateOrMinKeyCollectorSnapshot<TSnapshot> snapshot)
  {
    return new DelegateOrMinKeyCollector<>(
        comparator,
        Optional.ofNullable(snapshot.getSnapshot()).map(delegateFactory::fromSnapshot).orElse(null),
        snapshot.getMinKey()
    );
  }
}
