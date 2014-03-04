package org.janstenpickle.flume.serialization;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JsonDeserializer implements EventDeserializer {

    private static final Logger logger = LoggerFactory.getLogger
            (JsonDeserializer.class);

    private final InputStream in;
    private final Charset outputCharset;
    private volatile boolean isOpen;

    public static final String OUT_CHARSET_KEY = "outputCharset";
    public static final String CHARSET_DFLT = "UTF-8";


    JsonDeserializer(Context context, InputStream in) {
        this.in = in;
        this.outputCharset = Charset.forName(
                context.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
        this.isOpen = true;
    }

    @Override
    public Event readEvent() throws IOException {
        ensureOpen();
        JSONObject event = readLine();
        if (event == null) {
            return null;
        } else {
            Event e = EventBuilder.withBody(event.getString("body"), outputCharset);
            JSONObject jsonHeaders = event.getJSONObject("headers");
            Map<String, String> headers = new HashMap<>();
            for (Object key : jsonHeaders.keySet()) {
                String header = (String) key;
                String value = jsonHeaders.getString(header);
                headers.put(header,value);
            }
            e.setHeaders(headers);
            return e;
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    private JSONObject readLine() throws IOException {
        BufferedReader buffer=new BufferedReader(new InputStreamReader(in));
        String line=buffer.readLine();
        JSONTokener tokener = new JSONTokener(line);
        return new JSONObject(tokener);
    }

    @Override
    public List<Event> readEvents(int i) throws IOException {
        return null;
    }

    @Override
    public void mark() throws IOException {

    }

    @Override
    public void reset() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
