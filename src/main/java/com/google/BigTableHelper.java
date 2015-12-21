package com.google;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class BigTableHelper {
  private static final String PROJECT_ID = "ntnu-smartmedia";
  private static final String CLUSTER_ID = "smartmedia";
  private static final String ZONE = "europe-west1-c";

  private static Connection connection = null;

  public static Configuration conf() {
    Configuration config = HBaseConfiguration.create();

    config.setClass("hbase.client.connection.impl",
                    com.google.cloud.bigtable.hbase1_0.BigtableConnection.class,
                    org.apache.hadoop.hbase.client.Connection.class);

    config.set("google.bigtable.project.id", PROJECT_ID);
    config.set("google.bigtable.cluster.name", CLUSTER_ID);
    config.set("google.bigtable.zone.name", ZONE);

    return config;
  }
  public static Connection connect(Configuration config) throws IOException {
    connection = ConnectionFactory.createConnection(config);
    return connection;
  }
}