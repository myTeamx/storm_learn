package com.gjxx.email;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Sxs
 * @description bolt
 * @date 2019/9/30 11:47
 */
public class EmailExtractor extends BaseBasicBolt {

    /**
     * 当一个元组被发射到这个bolt时调用
     * @param tuple 元组
     * @param basicOutputCollector 发射器
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String commit = tuple.getStringByField("commit");
        String[] parts = commit.split(" ");
        basicOutputCollector.emit(new Values(parts[1]));
    }

    /**
     * 声明bolt发射字段的命名
     * @param outputFieldsDeclarer OutputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("email"));
    }
}
