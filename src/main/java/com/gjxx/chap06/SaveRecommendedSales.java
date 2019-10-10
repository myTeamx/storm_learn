package com.gjxx.chap06;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Preconditions;
import org.apache.storm.guava.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * @author Sxs
 * @description 保存推荐商品
 * @date 2019/10/9 14:29
 */
public class SaveRecommendedSales extends BaseBasicBolt {

    private String fileName;
    private File file;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        fileName = "files/recommendedSales.txt";
        file = new File(fileName);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String customerId = input.getStringByField("customerId");
        String sales = input.getStringByField("sales");

        // 数据校验
        Preconditions.checkNotNull(fileName, "Provided file name for writing must NOT be null.");
        Preconditions.checkNotNull(customerId, "Unable to write null contents.");
        Preconditions.checkNotNull(sales, "Unable to write null contents.");

        try {
            Files.write((customerId+sales).getBytes(), file);
        } catch (IOException e) {
            throw new ReportedFailedException(e);
        }
    }
}
