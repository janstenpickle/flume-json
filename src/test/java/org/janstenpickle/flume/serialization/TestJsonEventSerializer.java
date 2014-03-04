package org.janstenpickle.flume.serialization;

import com.google.common.base.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class TestJsonEventSerializer {
    File testFile = new File("src/test/resources/events.txt");

    @Test
    public void testWrite() throws Exception {

        OutputStream out = new FileOutputStream(testFile);
        EventSerializer serializer = new JsonEventSerializer(out, new Context());

        Map<String, String> headers = new HashMap<>();
        headers.put("header1", "value1");
        headers.put("header2", "value2");

        serializer.write(EventBuilder.withBody("event 1", Charsets.UTF_8, headers));
        serializer.write(EventBuilder.withBody("event 2", Charsets.UTF_8, headers));
        serializer.write(EventBuilder.withBody("event 3", Charsets.UTF_8, headers));
        serializer.flush();
        serializer.beforeClose();
        out.flush();
        out.close();

        BufferedReader reader = new BufferedReader(new FileReader(testFile));
        Assert.assertEquals("{\"headers\":{\"header2\":\"value2\",\"header1\":\"value1\"},\"body\":\"event 1\"}", reader.readLine());
        Assert.assertEquals("{\"headers\":{\"header2\":\"value2\",\"header1\":\"value1\"},\"body\":\"event 2\"}", reader.readLine());
        Assert.assertEquals("{\"headers\":{\"header2\":\"value2\",\"header1\":\"value1\"},\"body\":\"event 3\"}", reader.readLine());
        Assert.assertNull(reader.readLine());
        reader.close();

        FileUtils.forceDelete(testFile);
    }

    @Test
    public void testWriteWithSnappy() throws Exception {

        SnappyFramedOutputStream out = new SnappyFramedOutputStream(new FileOutputStream(testFile));
        EventSerializer serializer = new JsonEventSerializer(out, new Context());

        Map<String, String> headers = new HashMap<>();
        headers.put("header1", "value1");
        headers.put("header2", "value2");

        serializer.write(EventBuilder.withBody("event 1", Charsets.UTF_8, headers));
        serializer.write(EventBuilder.withBody("event 2", Charsets.UTF_8, headers));
        serializer.write(EventBuilder.withBody("event 3", Charsets.UTF_8, headers));
        serializer.flush();
        serializer.beforeClose();
        out.flush();
        out.close();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new SnappyFramedInputStream(new FileInputStream(testFile))));
        Assert.assertEquals("{\"headers\":{\"header2\":\"value2\",\"header1\":\"value1\"},\"body\":\"event 1\"}", reader.readLine());
        Assert.assertEquals("{\"headers\":{\"header2\":\"value2\",\"header1\":\"value1\"},\"body\":\"event 2\"}", reader.readLine());
        Assert.assertEquals("{\"headers\":{\"header2\":\"value2\",\"header1\":\"value1\"},\"body\":\"event 3\"}", reader.readLine());
        Assert.assertNull(reader.readLine());
        reader.close();

        FileUtils.forceDelete(testFile);
    }
}
