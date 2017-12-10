package com.ptb.gaia.etl.flume.process;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by eric on 16/7/11.
 */
public class MediaDynamicProcessTest {
    private ArticleDynamicProcess articleDynamicProcess = new ArticleDynamicProcess();

    public MediaDynamicProcessTest() throws ConfigurationException {
    }

    @Test
    public void addDaymic() {
        String data = "{\"comments\":-1,\"forwards\":-1,\"joins\":-1,\"likes\":9,\"nolikes\":-1,\"plat\":1,\"reads\":2049,\"time\":1468200303713,\"url\":\"http://mp.weixin.qq.com/s?__biz=MjM5MzUyNzcyMQ==&mid=2659301692&idx=5&sn=8933d8583d77658be15bd5f392d88a7f#rd\"}";
        String data1 = "{\"comments\":1566,\"forwards\":5138,\"joins\":-1,\"likes\":16526,\"nolikes\":-1,\"plat\":2,\"reads\":-1,\"time\":1468200113109,\"url\":\"http://m.weibo.cn/1266321801/DD81yqszc\"}";
        SimpleEvent simpleEvent = new SimpleEvent();
        simpleEvent.setBody(data.getBytes(Charset.forName("utf-8")));
        List<Event> events = new LinkedList<Event>();
        for (int i = 0; i < 500; i++) {
            events.add(simpleEvent);
        }

        Long start = System.currentTimeMillis();
        articleDynamicProcess.process(events);
        System.out.println(System.currentTimeMillis() - start);
    }
}