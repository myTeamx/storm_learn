package com.gjxx.chap06;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.gjxx.util.RedisUtils;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Sxs
 * @description 保存推荐商品
 * @date 2019/10/9 14:29
 */
public class SaveRecommendedSales extends BaseBasicBolt {

    private Jedis jedis = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        jedis = RedisUtils.getJedis();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String customerId = input.getStringByField("customerId");
        String sales = input.getStringByField("sales");

        try {
            jedis.lpush(customerId, sales);
        } catch (Exception e) {
            throw new ReportedFailedException(e);
        }
    }

    @Override
    public void cleanup() {
        RedisUtils.returnResource(jedis);
    }
}
