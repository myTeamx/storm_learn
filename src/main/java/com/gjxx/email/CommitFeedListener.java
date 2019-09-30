package com.gjxx.email;

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
 * @date 2019/9/30 11:23
 */
public class CommitFeedListener extends BaseRichSpout {

    /**
     * 发射元组
     */
    private SpoutOutputCollector outputCollector;

    /**
     * 从文件读取要提交的消息列表
     */
    private List<String> commits;

    /**
     * 在storm准备运行spout时调用
     * @param map 配置
     * @param topologyContext TopologyContext
     * @param spoutOutputCollector SpoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;

        try {

            this.commits = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("files/changelog.txt"),
                    Charset.defaultCharset().name());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 当spout准备读取下一个元组的时候，由storm调用
     */
    @Override
    public void nextTuple() {

        for (String commit : commits) {
            // 为每个提交消息，发射一个元组
            this.outputCollector.emit(new Values(commit));
        }

    }

    /**
     * 为spout发射的所有元组定义字段命名
     * @param outputFieldsDeclarer OutputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 表示spout发射一个命名为commit的字段
        outputFieldsDeclarer.declare(new Fields("commit"));
    }
}
