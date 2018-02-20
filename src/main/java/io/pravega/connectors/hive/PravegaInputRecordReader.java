/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.hive;

import io.pravega.client.ClientFactory;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.stream.Serializer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A RecordReader that can read events from an InputSplit as provided by {@link PravegaInputFormat}.
 */
@NotThreadSafe
public class PravegaInputRecordReader<V> implements RecordReader<EventKey, V> {

    private static final Logger log = LoggerFactory.getLogger(PravegaInputRecordReader.class);
    private ClientFactory clientFactory;
    private BatchClient batchClient;
    private PravegaInputSplit split;
    private SegmentIterator<V> iterator;
    // Pravega Serializer to deserialize events saved in pravega
    private Serializer<V> deserializer;
    private EventKey key;


    /**
     * Initializes RecordReader by InputSplit and TaskAttemptContext.
     *
     * <p>Connects to Pravega and prepares to read events in the InputSplit.
     *
     * @param split   InputSplit
     * @param context TaskAttemptContext
     */
    public PravegaInputRecordReader(InputSplit split, JobConf conf) throws IOException {
        this.split = (PravegaInputSplit) split;
        clientFactory = ClientFactory.withScope(conf.get(PravegaInputFormat.SCOPE_NAME), URI.create(conf.get(PravegaInputFormat.URI_STRING)));
        batchClient = clientFactory.createBatchClient();
        String deserializerClassName = conf.get(PravegaInputFormat.DESERIALIZER);
        try {
            Class<?> deserializerClass = Class.forName(deserializerClassName);
            deserializer = (Serializer<V>) deserializerClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.error("Exception when creating deserializer: {}", e);
            throw new IOException("Unable to create the event deserializer (" + deserializerClassName + ")", e);
        }
        iterator = batchClient.readSegment(this.split.getSegment(), deserializer, this.split.getStartOffset());
    }

    /**
     * Retrieves the next key/value pair from the InputSplit.
     *
     * @return next key/value exists or not
     */
    @Override
    public synchronized boolean next(EventKey k, V v) throws IOException {
        if (iterator.hasNext()) {
            k.Set(split, iterator.getOffset());
			key = k;
	        try {
			  Text sv = (Text)v;
              sv.set(iterator.next().toString());
            } catch(ClassCastException e) {
				log.error("### CAST FAILED");
            }
            log.error("### Key: {}, Value: {} ({})", k, v, v.getClass().getName());
            return true;
        }
        return false;
    }

    /**
     * Gets the key associated with the current key/value pair.
     */
    @Override
    public EventKey createKey() {
        //return key;
		return new EventKey();
    }

    /**
     * Gets the value associated with the current key/value pair.
     */
    @Override
    public V createValue() {
		return ReflectionUtils.newInstance((Class<? extends V>)Text.class, null);
    }

    @Override
    public synchronized float getProgress() throws IOException {
        if (key != null && split.getLength() > 0) {
            return ((float) (key.getOffset() - split.getStartOffset())) / split.getLength();
        }
        return 1f;
    }

    @Override
    public synchronized void close() throws IOException {
        if (iterator != null) {
            iterator.close();
        }
        if (clientFactory != null) {
            clientFactory.close();
        }
    }

	@Override
	public long getPos() throws IOException {
        if (key != null) {
		   return key.getOffset();
	    }
		return 0;
    }
}
