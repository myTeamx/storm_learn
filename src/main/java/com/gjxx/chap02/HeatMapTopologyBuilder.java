package com.gjxx.chap02;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author Sxs
 * @description 创建topology
 * @date 2019/9/30 18:30
 */
public class HeatMapTopologyBuilder {

    public StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("checkins", new Checkins());
        builder.setBolt("geocode-lookup", new GeocodeLookup())
                .shuffleGrouping("checkins");
        builder.setBolt("heatmap-builder", new HeatMapBuilder())
                .globalGrouping("geocode-lookup");
        builder.setBolt("persistor", new Persistor())
                .shuffleGrouping("heatmap-builder");
        return builder.createTopology();
    }

}
