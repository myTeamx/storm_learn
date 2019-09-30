package com.gjxx.email;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sxs
 * @description bolt
 * @date 2019/9/30 11:50
 */
public class EmailCounter extends BaseBasicBolt {

    /**
     * 内存中对应email和统计计算器的映射表
     */
    private Map<String, Integer> counts;

    /**
     * storm在这个bolt准备运行时调用
     * @param stormConf Map
     * @param context TopologyContext
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        counts = new HashMap<>(16);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String email = tuple.getStringByField("email");
        Integer count = counts.getOrDefault(email, 0);
        counts.put(email, count + 1);
        printCounts();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 不发射，所以不需要给字段命名
    }

    private void printCounts() {
        counts.forEach((k, v) -> System.out.println(k + "->" + v));
    }
}
