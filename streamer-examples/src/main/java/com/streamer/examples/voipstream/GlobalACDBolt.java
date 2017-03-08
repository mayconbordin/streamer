package com.streamer.examples.voipstream;

import com.streamer.core.Operator;
import com.streamer.core.Tuple;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GlobalACDBolt extends Operator {
    private static final Logger LOG = Logger.getLogger(GlobalACDBolt.class);
    /*
    private OutputCollector collector;
    private VariableEWMA avgCallDuration;
    private double age;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TIMESTAMP_FIELD, AVERAGE_FIELD));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        age = ConfigUtility.getDouble(stormConf, "voipstream.acd.age"); //86400s = 24h
        avgCallDuration = new VariableEWMA(age);
    }

    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(RECORD_FIELD);
        long timestamp = cdr.getAnswerTime().getMillis()/1000;

        avgCallDuration.add(cdr.getCallDuration());
        collector.emit(new Values(timestamp, avgCallDuration.getAverage()));
    }*/

    public void process(Tuple tuple) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}