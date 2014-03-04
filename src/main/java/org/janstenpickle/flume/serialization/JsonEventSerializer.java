package org.janstenpickle.flume.serialization;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class JsonEventSerializer implements EventSerializer {

    private final static Logger logger =
            LoggerFactory.getLogger(JsonEventSerializer.class);

    public static final String OUT_CHARSET_KEY = "outputCharset";
    public static final String CHARSET_DFLT = "UTF-8";

    private final OutputStream out;
    private final Charset outputCharset;

    public JsonEventSerializer(OutputStream out, Context ctx) {
        this.outputCharset = Charset.forName(
                ctx.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
        this.out = out;
    }

    @Override
    public void afterCreate() throws IOException {

    }

    @Override
    public void afterReopen() throws IOException {

    }

    @Override
    public void write(Event e) throws IOException {
        e.getHeaders();

        JSONObject event = new JSONObject();
        event.put("headers", e.getHeaders());
        event.put("body", new String(e.getBody(), outputCharset));

        out.write(event.toString().getBytes());
        out.write('\n');
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void beforeClose() throws IOException {

    }

    @Override
    public boolean supportsReopen() {
        return true;
    }

    public static class Builder implements EventSerializer.Builder {

        @Override
        public EventSerializer build(Context context, OutputStream out) {
            JsonEventSerializer s = new JsonEventSerializer(out, context);
            return s;
        }

    }
}
