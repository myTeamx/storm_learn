package com.gjxx.email;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * @author Sxs
 * @description storm topology
 * @date 2019/9/30 12:00
 */
public class LocalTopologyRunner {

    private static final int TEN_MINUTES = 600000;

    public static void main(String[] args) {

        // 用于将spout与bolts连到一起
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("commit-feed-listener", new CommitFeedListener());

        builder.setBolt("email-extractor", new EmailExtractor())
                // 定义在提交消息监听器与email提取器之间的数据流
                .shuffleGrouping("commit-feed-listener");

        builder.setBolt("email-counter", new EmailCounter())
                // 定义在email提取器与email计数器之间的数据流之间的数据流
                .fieldsGrouping("email-extractor", new Fields("email"));

        Config config = new Config();
        config.setDebug(true);

        // 创建拓扑
        StormTopology topology = builder.createTopology();

        // 提交拓扑和其配置到本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("github-commit-count-topology", config, topology);

        // 休眠一段时间，杀掉拓扑，并关闭本地集群
        Utils.sleep(TEN_MINUTES);
        cluster.killTopology("github-commit-count-topology");
        cluster.shutdown();

    }

}
