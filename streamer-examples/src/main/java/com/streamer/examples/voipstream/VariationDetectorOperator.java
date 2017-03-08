package com.streamer.examples.voipstream;

import com.streamer.core.Operator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.utils.bloom.BloomFilter;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.Config;
import com.streamer.utils.Configuration;
import java.util.Date;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VariationDetectorOperator extends Operator {
    private static final Logger LOG = LoggerFactory.getLogger(VariationDetectorOperator.class);
    
    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private int approxInsertSize;
    private double falsePostiveRate;
    private double cycleThreshold;

    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
        
        approxInsertSize = config.getInt(Config.VAR_DETECT_APROX_SIZE);
        falsePostiveRate = config.getInt(Config.VAR_DETECT_ERROR_RATE);
        
        detector = new BloomFilter<String>(falsePostiveRate, approxInsertSize);
        learner  = new BloomFilter<String>(falsePostiveRate, approxInsertSize);
        
        cycleThreshold = detector.size()/Math.sqrt(2);
    }

    public void process(Tuple input) {
        String callingNumber = input.getString("callingNumber");
        String calledNumber  = input.getString("calledNumber");
        Date answerTime      = (Date) input.getValue("answerTime");
        
        String key = String.format("%s:%s", callingNumber, calledNumber);
        boolean newCallee = false;
        
        // add pair to learner
        learner.add(key);
        
        // check if the pair exists
        // if not, add to the detector
        if (!detector.membershipTest(key)) {
            detector.add(key);
            newCallee = true;
        }
        
        // if number of non-zero bits is above threshold, rotate filters
        if (detector.getNumNonZero() > cycleThreshold) {
            rotateFilters();
        }
        
        Values values = new Values(callingNumber,calledNumber, answerTime, newCallee);
        
        emit(input, values);
        emit("variationsBackup", input, values);
    }
    
    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}