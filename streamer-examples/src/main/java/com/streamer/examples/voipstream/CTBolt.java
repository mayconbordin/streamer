package com.streamer.examples.voipstream;

import com.streamer.core.Tuple;
import org.apache.log4j.Logger;

/**
 * Per-user total call time
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CTBolt extends AbstractFilterBolt {
    private static final Logger LOG = Logger.getLogger(CTBolt.class);
/*
    public CTBolt(String configPrefix) {
        super(configPrefix, CALLTIME_FIELD);
    }
    
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(RECORD_FIELD);
        boolean newCallee = input.getBooleanByField(NEW_CALLEE_FIELD);
        
        if (cdr.isCallEstablished() && newCallee) {
            String caller = input.getStringByField(CALLING_NUM_FIELD);
            long timestamp = cdr.getAnswerTime().getMillis()/1000;

            filter.add(caller, cdr.getCallDuration(), timestamp);
            double calltime = filter.estimateCount(caller, timestamp);

            LOG.info(String.format("CallTime: %f", calltime));
            collector.emit(new Values(caller, timestamp, calltime, cdr));
        }
    }*/

    public void process(Tuple tuple) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}