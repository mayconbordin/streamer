package com.streamer.examples.voipstream;

import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.base.task.AbstractTask;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.Config;
import com.streamer.partitioning.Fields;
import com.streamer.utils.Configuration;
import org.slf4j.Logger;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class VoIPSTREAMTopology extends AbstractTask {
    private int varDetectThreads;
    private int ecrThreads;
    private int rcrThreads;
    private int encrThreads;
    private int ecr24Threads;
    private int ct24Threads;
    private int fofirThreads;
    private int urlThreads;
    private int globalAcdThreads;
    private int acdThreads;
    private int scorerThreads;

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        varDetectThreads = config.getInt(Config.VAR_DETECT_THREADS, 1);
        ecrThreads       = config.getInt(Config.ECR_THREADS, 1);
        rcrThreads       = config.getInt(Config.RCR_THREADS, 1);
        encrThreads      = config.getInt(Config.ENCR_THREADS, 1);
        ecr24Threads     = config.getInt(Config.ECR24_THREADS, 1);
        ct24Threads      = config.getInt(Config.CT24_THREADS, 1);
        fofirThreads     = config.getInt(Config.FOFIR_THREADS, 1);
        urlThreads       = config.getInt(Config.URL_THREADS, 1);
        //globalAcdThreads = config.getInt(Config.GLOBAL_ACD_THREADS, 1);
        acdThreads       = config.getInt(Config.ACD_THREADS, 1);
        scorerThreads    = config.getInt(Config.SCORER_THREADS, 1);
    }
    
    public void initialize() {
        Stream cdrs = builder.createStream("cdrs", new Schema().keys("callingNumber", "calledNumber").fields("answerTime", "callDuration", "callEstablished"));
        Stream variations = builder.createStream("variations", new Schema("callingNumber", "calledNumber", "answerTime", "newCallee"));
        Stream variationsBackup = builder.createStream("variationsBackup", new Schema("callingNumber", "calledNumber", "answerTime", "newCallee"));
        Stream establishedCallRates = builder.createStream("establishedCallRates", new Schema("callingNumber", "answerTime", "rate"));
        Stream receivedCallRates = builder.createStream("receivedCallRates", new Schema("callingNumber", "answerTime", "rate"));
        Stream newCalleeRates = builder.createStream("newCalleeRates", new Schema());        
        //builder.setSource("source", source, sourceThreads);
        builder.publish("source", cdrs);
        //builder.setTupleRate("source", sourceRate);
        
        builder.setOperator("variationDetector", new VariationDetectorOperator(), varDetectThreads);
        builder.groupByKey("variationDetector", cdrs);
        builder.publish("variationDetector", variations);
        builder.publish("variationDetector", variationsBackup);
        
        // Filters -----------------------------------------------------------//
        
        builder.setOperator("ecr", new ECROperator(), ecrThreads);
        builder.groupBy("ecr", variations, new Fields("callingNumber"));
        builder.publish("ecr", establishedCallRates);
        
        builder.setOperator("rcr", new RCROperator(), rcrThreads);
        builder.groupBy("rcr", variations, new Fields("callingNumber"));
        builder.groupBy("rcr", variationsBackup, new Fields("calledNumber"));
        builder.publish("rcr", receivedCallRates);
        
        builder.setOperator("encr", new ENCROperator(), encrThreads);
        builder.groupBy("encr", variations, new Fields("callingNumber"));
        
/*

        builder.setBolt(ENCR_BOLT, new ENCRBolt(), encrThreads)
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        builder.setBolt(ECR24_BOLT, new ECRBolt("ecr24"), ecr24Threads)
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, new Fields(CALLING_NUM_FIELD));
 
        builder.setBolt(CT24_BOLT, new CTBolt("ct24"), ct24Threads)
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        
        // Modules
        
        builder.setBolt(FOFIR_BOLT, new FoFiRBolt(), fofirThreads)
               .fieldsGrouping(RCR_BOLT, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(ECR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        
        builder.setBolt(URL_BOLT, new URLBolt(), urlThreads)
               .fieldsGrouping(ENCR_BOLT, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(ECR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        // the average must be global, so there must be a single instance doing that
        // perhaps a separate bolt, or if multiple bolts are used then a merger should
        // be employed at the end point.
        builder.setBolt(GLOBAL_ACD_BOLT, new GlobalACDBolt(), globalAcdThreads)
               .fieldsGrouping(VARIATION_DETECTOR_BOLT, new Fields(CALLING_NUM_FIELD));
        
        builder.setBolt(ACD_BOLT, new ACDBolt(), acdThreads)
               .fieldsGrouping(ECR24_BOLT, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(CT24_BOLT, new Fields(CALLING_NUM_FIELD))
               .allGrouping(GLOBAL_ACD_BOLT);
        
        
        // Score
        builder.setBolt(SCORER_BOLT, new ScoreBolt(), scorerThreads)
               .fieldsGrouping(FOFIR_BOLT, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(URL_BOLT, new Fields(CALLING_NUM_FIELD))
               .fieldsGrouping(ACD_BOLT, new Fields(CALLING_NUM_FIELD));
        
        builder.setBolt(FILE_SINK, new FileSink(spoutPath), sinkThreads)
               .fieldsGrouping(SCORER_BOLT, new Fields(CALLING_NUM_FIELD));
        
        return builder.createTopology();*/
    }

    @Override
    public Logger getLogger() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getConfigPrefix() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
