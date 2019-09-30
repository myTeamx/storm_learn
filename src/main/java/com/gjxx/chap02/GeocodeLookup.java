package com.gjxx.chap02;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sxs
 * @description bolt
 * @date 2019/9/30 17:42
 */
public class GeocodeLookup extends BaseBasicBolt {

    private Map<String, String> decode;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        decode = new HashMap<>(16);
        decode.put("287 Hudson St New York NY 10013", "40.72612, -74.001396");

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String address = input.getStringByField("address");
        Long time = input.getLongByField("time");
        String latLng = decode.getOrDefault(address, "0, 0");
        collector.emit(new Values(time, latLng));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "decode"));
    }

}
