package com.gjxx.chap06;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;

/**
 * @author Sxs
 * @description TODO
 * @date 2019/10/9 11:51
 */
public class CustomerRetrievalSpout extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void nextTuple() {

        String customerId = UUID.randomUUID().toString().replace("-", "");
        outputCollector.emit(new Values(customerId));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("customerId"));
    }

}
