package com.streamer.examples.voipstream;

import com.streamer.core.Tuple;
import org.apache.log4j.Logger;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ACDBolt extends AbstractScoreBolt {
    private static final Logger LOG = Logger.getLogger(ACDBolt.class);
    /*
    private double avg;

    public ACDBolt() {
        super("acd");
    }

    public void execute(Tuple input) {
        Source src = parseComponentId(input.getSourceComponent());
        
        if (src == Source.GACD) {
            avg = input.getDoubleByField(AVERAGE_FIELD);
        } else {
            CallDetailRecord cdr = (CallDetailRecord) input.getValueByField("record");
            String number  = input.getString(0);
            long timestamp = input.getLong(1);
            double rate    = input.getDouble(2);

            String key = String.format("%s:%d", number, timestamp);

            if (map.containsKey(key)) {
                Entry e = map.get(key);
                e.set(src, rate);

                if (e.isFull()) {
                    // calculate the score for the ratio
                    double ratio = (e.get(Source.CT24)/e.get(Source.ECR24))/avg;
                    double score = score(thresholdMin, thresholdMax, ratio);
                    
                    LOG.info(String.format("T1=%f; T2=%f; CT24=%f; ECR24=%f; AvgCallDur=%f; Ratio=%f; Score=%f", 
                        thresholdMin, thresholdMax, e.get(Source.CT24), e.get(Source.ECR24), avg, ratio, score));

                    collector.emit(new Values(number, timestamp, score, cdr));
                    map.remove(key);
                } else {
                    LOG.warn(String.format("Inconsistent entry: source=%s; %s",
                            input.getSourceComponent(), e.toString()));
                }
            } else {
                Entry e = new Entry(cdr);
                e.set(src, rate);
                map.put(key, e);
            }
        }
    }
    
    @Override
    protected Source[] getFields() {
        return new Source[]{Source.CT24, Source.ECR24};
    }*/

    public ACDBolt(String configPrefix) {
        super(configPrefix);
    }

    public void process(Tuple tuple) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}