package com.streamer.examples.reinforcementlearner;

import com.streamer.base.constants.BaseConstants;
import com.streamer.base.constants.BaseConstants.BaseConfig;

public interface ReinforcementLearnerConstants extends BaseConstants {
    String PREFIX = "rl";
    
    interface Config extends BaseConfig {
        String LEARNER_THREADS                = "rl.learner.threads";
        String LEARNER_TYPE                   = "rl.learner.type";
        String LEARNER_ACTIONS                = "rl.learner.actions";
        
        String RANDOM_SELECTION_PROB          = "rl.random.selection.prob";
        String PROB_RED_ALGORITHM             = "rl.prob.reduction.algorithm";
        String PROB_RED_CONSTANT              = "rl.prob.reduction.constant";
        
        String BIN_WIDTH                      = "rl.bin.width";
        String CONFIDENCE_LIMIT               = "rl.confidence.limit";
        String MIN_CONFIDENCE_LIMIT           = "rl.min.confidence.limit";
        String CONFIDENCE_LIMIT_RED_STEP      = "rl.confidence.limit.reduction.step";
        String CONFIDENCE_LIMIT_RED_ROUND_INT = "rl.confidence.limit.reduction.round.interval";
        String MIN_DIST_SAMPLE                = "rl.min.reward.distr.sample";
        
        String MIN_SAMPLE_SIZE                = "rl.min.sample.size";
        String MAX_REWARD                     = "rl.max.reward";
        
    }
    
    interface Field {
        String EVENT_ID = "eventID";
        String ACTION_ID = "actionID";
        String ACTIONS = "actions";
        String REWARD = "reward";
        String ROUND_NUM = "roundNum";
    }
    
    interface Component extends BaseComponent {
        String EVENT_SOURCE = "eventSource";
        String REWARD_SOURCE = "rewardSource";
        String LEARNER = "reinforcementLearner";
    }
    
    interface Learner {
        String INTERVAL_ESTIMATOR = "intervalEstimator";
        String SAMPSON_SAMPLER = "sampsonSampler";
        String OPT_SAMPSON_SAMPLER = "optimisticSampsonSampler";
        String RANDOM_GREEDY = "randomGreedyLearner";
    }
    
    interface Streams {
        String EVENTS  = "eventStream";
        String REWARDS = "rewardStream";
        String ACTIONS = "actionStream";
    }
}
