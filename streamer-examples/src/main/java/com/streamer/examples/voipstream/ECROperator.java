package com.streamer.examples.voipstream;

import com.streamer.core.Operator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.utils.bloom.ODTDBloomFilter;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.Config;
import com.streamer.utils.Configuration;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-user received call rate.
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ECROperator extends Operator {
    private static final Logger LOG = LoggerFactory.getLogger(ECROperator.class);

    private ODTDBloomFilter filter;
    
    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
        /*
        int numElements       = config.getInt(Config.ECR_NUM_ELEMENTS);
        int bucketsPerElement = config.getInt(Config.ECR_BUCKETS_ELEMENT);
        int bucketsPerWord    = config.getInt(Config.ECR_BUCKETS_WORD);
        double beta           = config.getDouble(Config.ECR_BETA);
        
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);*/
    }

    public void process(Tuple input) {
        boolean callEstablished = input.getBoolean("callEstablished");
        
        if (callEstablished) {
            String caller  = input.getString("callingNumber");
            long timestamp = ((Date) input.getValue("answerTime")).getTime()/1000;

            // add numbers to filters
            filter.add(caller, 1, timestamp);
            double ecr = filter.estimateCount(caller, timestamp);

            emit(new Values(caller, timestamp, ecr));
        }
    }
}
