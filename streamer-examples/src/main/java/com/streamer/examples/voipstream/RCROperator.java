package com.streamer.examples.voipstream;

import com.streamer.core.Operator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.utils.bloom.ODTDBloomFilter;
import com.streamer.utils.Configuration;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-user received call rate.
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class RCROperator extends Operator {
    private static final Logger LOG = LoggerFactory.getLogger(RCROperator.class);
    
    private ODTDBloomFilter filter;
    
    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
        /*
        int numElements       = config.getInt(VoIPSTREAMConstants.Config.RCR_NUM_ELEMENTS);
        int bucketsPerElement = config.getInt(VoIPSTREAMConstants.Config.RCR_BUCKETS_ELEMENT);
        int bucketsPerWord    = config.getInt(VoIPSTREAMConstants.Config.RCR_BUCKETS_WORD);
        double beta           = config.getDouble(VoIPSTREAMConstants.Config.RCR_BETA);
        
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);*/
    }

    public void process(Tuple input) {
        boolean callEstablished = input.getBoolean("callEstablished");
        
        if (callEstablished) {
            long timestamp = ((Date) input.getValue("answerTime")).getTime()/1000;
            
            if (input.getStreamId().equals("variations")) {
                String callee = input.getString("calledNumber");
                filter.add(callee, 1, timestamp);
            }

            else if (input.getStreamId().equals("variationsBackup")) {
                String caller = input.getString("callingNumber");
                double rcr = filter.estimateCount(caller, timestamp);
                
                emit(new Values(caller, timestamp, rcr));
            }
        }
    }
}
