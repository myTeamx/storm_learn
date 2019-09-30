package com.gjxx.chap02;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * @author Sxs
 * @description bolt 将数据寄放在内存里
 * @date 2019/9/30 17:50
 */
public class HeatMapBuilder extends BaseBasicBolt {

    private Map<Long, List<String>> heatMaps;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        heatMaps = new HashMap<>(16);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (isTickTuple(input)) {
            emitHeatMap(collector);
        } else {
            Long time = input.getLongByField("time");
            String decode = (String)input.getValueByField("decode");
            Long timeInterval = selectTimeInterval(time);
            List<String> checkins = getCheckinsForInterval(timeInterval);
            checkins.add(decode);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "hotzones"));
    }

    /**
     * 配置心跳元组
     * @return 配置
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        // 配置心跳元组，60s
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        return config;
    }

    /**
     * 发射在当前时间15s之前，或者之内的数据，并在map中移除
     * @param outputCollector 发射器
     */
    private void emitHeatMap(BasicOutputCollector outputCollector) {
        long now = System.currentTimeMillis();
        Long emitUpToTimeInterval = selectTimeInterval(now);
        heatMaps.forEach((k, v) -> {
            if (k <= emitUpToTimeInterval) {
                List<String> hotzones = heatMaps.remove(k);
                outputCollector.emit(new Values(k, hotzones));
            }
        });
    }

    /**
     * 心跳元组依靠系统组件来实现心跳流的发射，而不是使用我们自己拓扑上
     * 默认流而定义的组件
     * @param tuple 元组
     * @return 是否心跳流
     */
    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();
        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    /**
     * 从15s的整数倍到下一个整数倍之间的所有时间，都返回同一个值，
     * 把15s内的数据都划分到map中同一个key
     * @param time 时间
     * @return map中的key
     */
    private Long selectTimeInterval(Long time) {
        return time / (15 * 1000);
    }

    private List<String> getCheckinsForInterval(Long timeInterval) {
        List<String> hotzones = this.heatMaps.get(timeInterval);
        if (Objects.isNull(hotzones)) {
            hotzones = new ArrayList<>();
            heatMaps.put(timeInterval, hotzones);
        }
        return hotzones;
    }

}
