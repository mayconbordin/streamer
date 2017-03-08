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
 * Per-user new callee rate
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ENCROperator extends Operator {
    private static final Logger LOG = LoggerFactory.getLogger(ENCROperator.class);

    private ODTDBloomFilter filter;
    
    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
        /*
        int numElements       = config.getInt(VoIPSTREAMConstants.Config.ENCR_NUM_ELEMENTS);
        int bucketsPerElement = config.getInt(VoIPSTREAMConstants.Config.ENCR_BUCKETS_ELEMENT);
        int bucketsPerWord    = config.getInt(VoIPSTREAMConstants.Config.ENCR_BUCKETS_WORD);
        double beta           = config.getDouble(VoIPSTREAMConstants.Config.ENCR_BETA);
        
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);*/
    }

    public void process(Tuple input) {
        boolean callEstablished = input.getBoolean("callEstablished");
        boolean newCallee = input.getBoolean("newCallee");
        
        if (callEstablished && newCallee) {
            String caller = input.getString("callingNumber");
            long timestamp = ((Date) input.getValue("answerTime")).getTime()/1000;

            filter.add(caller, 1, timestamp);
            double rate = filter.estimateCount(caller, timestamp);

            emit(new Values(caller, timestamp, rate));
        }
    }
}