package com.ptb.gaia.etl.flume.sink;

import com.ptb.gaia.etl.flume.process.IProcess;
import com.ptb.gaia.etl.flume.utils.SysLog;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import static com.ptb.gaia.etl.flume.utils.Constant.FDBatchSize;
import static com.ptb.gaia.etl.flume.utils.Constant.FDProcessClass;
import static com.ptb.gaia.etl.flume.utils.SysLog.Action.FLUME_SINK_PROCESS;
import static com.ptb.gaia.etl.flume.utils.SysLog.Action.FLUME_SINK_PROCESS_ERROR;

/**
 * Created by eric on 16/6/23.
 */
public class MongoSink extends AbstractSink implements Configurable {
    static Logger logger = LoggerFactory.getLogger(MongoSink.class);

    private int batchSize;
    private String ProcessClass;
    private IProcess iProcess;
    Configuration conf = new PropertiesConfiguration();

    @Override
    public synchronized void start() {
        super.start();

        try {
            iProcess = (IProcess) Class.forName(ProcessClass).newInstance();
            iProcess.configure(conf);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {

        List<Event> events = new LinkedList();
        events.clear();
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        Status status = Status.READY;

        try {
            txn.begin();
            Long begin = System.currentTimeMillis();
            for (int i = 0; i < batchSize; i++) {
                Event event = ch.take();
                if (event == null) {
                    status = Status.BACKOFF;
                    break;
                }
                events.add(event);
            }
            if (events.size() > 0) {
                iProcess.process(events);
            }
            SysLog.info(new SysLog.Action(FLUME_SINK_PROCESS, String.format("[%s] %.2f pps", ProcessClass, events.size() / ((System.currentTimeMillis() - begin) / 1000.0))));
            txn.commit();
        } catch (Throwable t) {
            SysLog.error(new SysLog.Action(FLUME_SINK_PROCESS, String.format("ProcessClass [%s]", ProcessClass)), FLUME_SINK_PROCESS_ERROR, t.getMessage());
            logger.error("proccess message error", t);
            txn.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            if (txn != null) {
                txn.close();
            }
        }
        return status;
    }


    @Override
    public void configure(Context context) {
        batchSize = context.getInteger(FDBatchSize);
        ProcessClass = context.getString(FDProcessClass);
    }
}
