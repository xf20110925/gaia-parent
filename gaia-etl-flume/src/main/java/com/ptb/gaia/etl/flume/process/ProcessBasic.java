package com.ptb.gaia.etl.flume.process;

import com.ptb.gaia.service.IGaia;
import com.ptb.gaia.service.IGaiaImpl;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

/**
 * Created by eric on 16/6/23.
 */
public abstract class ProcessBasic implements IProcess {
    protected IGaia iGaia;

    public Configuration conf;

    public ProcessBasic() throws ConfigurationException {
        iGaia = new IGaiaImpl();
    }

    @Override
    public void configure(Configuration conf) {
        this.conf = conf;
    }
}
