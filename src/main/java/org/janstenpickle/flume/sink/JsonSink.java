package org.janstenpickle.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class JsonSink extends AbstractSink implements Configurable {
    @Override
    public void configure(Context context) {

    }

    @Override
    public Status process() throws EventDeliveryException {
        return null;
    }
}
