package com.gjxx.chap02;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.io.IOUtils;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * @author Sxs
 * @description spout
 * @date 2019/9/30 17:24
 */
public class Checkins extends BaseRichSpout {

    /**
     * 使用list来存储从checkins.txt文件读取的静态数据
     */
    private List<String> checkins;

    /**
     * 用于定位列表中的当前位置，因为稍后需要回收checkins的静态列表
     */
    private int nextEmitIndex;

    /**
     * 发射器
     */
    private SpoutOutputCollector outputCollector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;
        this.nextEmitIndex = 0;

        try {
            this.checkins = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("files/checkins.txt"),
                    Charset.defaultCharset());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void nextTuple() {
        String checkin = this.checkins.get(this.nextEmitIndex);
        String[] parts = checkin.split(",");
        Long time = Long.valueOf(parts[0]);
        String address = parts[1];
        this.outputCollector.emit(new Values(time, address));
        nextEmitIndex = (nextEmitIndex + 1) % this.checkins.size();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "address"));
    }
}
