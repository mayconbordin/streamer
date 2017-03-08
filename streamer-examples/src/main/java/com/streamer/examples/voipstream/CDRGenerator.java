package com.streamer.examples.voipstream;

import com.streamer.base.source.generator.Generator;
import com.streamer.core.Values;
import com.streamer.examples.utils.RandomUtil;
import com.streamer.examples.voipstream.CDRDataGenerator;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.Config;
import com.streamer.utils.Configuration;
import java.util.Random;
import java.util.UUID;
import org.joda.time.DateTime;

/**
 *
 * @author mayconbordin
 */
public class CDRGenerator extends Generator {
    private String[] phoneNumbers;
    private int population;
    private double errorProb;
    private Random rand = new Random();
    private int count = 0;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        population = config.getInt(Config.GENERATOR_POPULATION, 50);
        errorProb  = config.getDouble(Config.GENERATOR_POPULATION, 0.05);
        
        phoneNumbers = new String[population];
        
        for (int i=0; i<population; i++) {
            phoneNumbers[i] = CDRDataGenerator.phoneNumber("US", 11);
        }
    }
    
    @Override
    public Values generate() {
        String callingNumber = pickNumber();
        String calledNumber = pickNumber(callingNumber);
        DateTime answerTime = DateTime.now().plusMinutes(RandomUtil.randInt(0, 60));
        int callDuration = RandomUtil.randInt(0, 60 * 5);
        boolean callEstablished = (CDRDataGenerator.causeForTermination(errorProb) == CDRDataGenerator.TERMINATION_CAUSE_OK);
        
        Values values = new Values(callingNumber, calledNumber, answerTime, callDuration, callEstablished);
        values.setId(count++);
        return values;
    }
    
    private String pickNumber(String excluded) {
        String number = "";
        while (number.isEmpty() || number.equals(excluded)) {
            number = phoneNumbers[rand.nextInt(population)];
        }
        return number;
    }
    
    private String pickNumber() {
        return phoneNumbers[rand.nextInt(population)];
    }
}
