package org.janstenpickle.flume.serialization;


import org.apache.commons.io.IOUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyInputStream;

import java.io.*;

public class TestJsonEventDeserializer {

    private String mini;
    private byte[] compressed;

    @Before
    public void setup() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"headers\":{\"header2\":\"value2\",\"header1\":\"value1\"},\"body\":\"event 1\"}\n");
        sb.append("{\"headers\":{\"header2\":\"value2\",\"header1\":\"value1\"},\"body\":\"event 2\"}\n");
        sb.append("{\"headers\":{\"header2\":\"value2\",\"header1\":\"value1\"},\"body\":\"event 3\"}\n");

        mini = sb.toString();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(new SnappyFramedInputStream(new ByteArrayInputStream(mini.getBytes())), out);
        compressed = out.toByteArray();
    }

    @Test
    public void testRead() throws Exception {
        EventDeserializer deserializer = new JsonDeserializer(new Context(), new ByteArrayInputStream(mini.getBytes()));

        Event event = deserializer.readEvent();

        System.out.println(event);
    }

    @Test
    public void testSnappyRead() throws Exception {
        EventDeserializer deserializer = new JsonDeserializer(new Context(), new SnappyFramedInputStream(new ByteArrayInputStream(compressed)));

        Event event = deserializer.readEvent();

        System.out.println(event);
    }
}
