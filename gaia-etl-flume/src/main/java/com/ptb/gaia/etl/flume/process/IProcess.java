package com.ptb.gaia.etl.flume.process;

import org.apache.commons.configuration.Configuration;
import org.apache.flume.Event;

import java.util.List;

/**
 * Created by eric on 16/6/23.
 */
public interface IProcess {
    void configure(Configuration conf);
    void process(List<Event> events);
}
