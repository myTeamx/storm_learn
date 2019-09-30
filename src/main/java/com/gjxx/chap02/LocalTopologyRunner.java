package com.gjxx.chap02;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

/**
 * @author Sxs
 * @description 提交topology
 * @date 2019/9/30 18:34
 */
public class LocalTopologyRunner {

    private static final int TEN_MINUTES = 600000;

    public static void main(String[] args) {
        Config config = new Config();
//        config.setDebug(true);

        StormTopology topology = new HeatMapTopologyBuilder().build();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("local-heatmap", config, topology);

        // 休眠一段时间，杀掉拓扑，并关闭本地集群
        Utils.sleep(TEN_MINUTES);
        localCluster.killTopology("local-heatmap");
        localCluster.shutdown();
    }

}
