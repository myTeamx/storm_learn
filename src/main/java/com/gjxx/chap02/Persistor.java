package com.gjxx.chap02;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author Sxs
 * @description bolt
 * @date 2019/9/30 18:26
 */
public class Persistor extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("====================");
        System.out.println(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    /**
     * 在storm的拓扑停止之后，执行的操作
     */
    @Override
    public void cleanup() {

    }
}
