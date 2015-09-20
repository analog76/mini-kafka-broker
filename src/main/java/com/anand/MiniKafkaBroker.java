package com.anand;

import com.anand.properties.KafkaProperties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by anand.ranganathan on 9/17/15.
 */
public class MiniKafkaBroker implements KafkaProperties {

    KafkaServer broker = null;

    private File kafkaDir= null;
    private MiniZKServer zk = null;
    Properties kafkaProperites = new Properties();


    public MiniKafkaBroker(){
        loadProperties();
    }

    public MiniKafkaBroker(Properties baseProperties){
        loadProperties();
        baseProperties = baseProperties;
    }


    private void loadProperties(){
        kafkaDir = getDirectory("/mini-kafka/kafka-logs");
        kafkaProperites = new Properties();

        kafkaProperites.setProperty("hostname", this.HOSTNAME);
        kafkaProperites.setProperty("port", this.PORT);
        kafkaProperites.setProperty("broker.id", this.BROKER_ID);
        kafkaProperites.setProperty("log.dir", kafkaDir.getAbsolutePath());
        kafkaProperites.setProperty("enable.zookeeper", "false");
        kafkaProperites.setProperty("zookeeper.connect",getZkConnection());
    }


    private File getDirectory(String path) {
        return Utilities.getDirectory(path);
    }


    private String getZkConnection(){
        String zkConnectionString = null;
        try {
              zk =  new MiniZKServer();
            zk.startZookeeper();
            zkConnectionString = zk.getConnectionString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return zkConnectionString;
    }



    public void startBroker() {
            startBroker(kafkaProperites);
    }

    private void startBroker(Properties props) {
        broker = new KafkaServer(new KafkaConfig(props), new SystemTime());
        broker.startup();
     }

    private KafkaServer getBroker(){
        return this.broker;
    }

    public String getConnectionString(){
        return "localhost:"+kafkaProperites.getProperty("port");
    }



    public void shutDown(){
        System.out.println("broker shutdown begins for  "+ this.getConnectionString());

        this.broker.shutdown();
        System.out.println("broker shutdown completed  "+ this.getConnectionString());

        this.zk.shutdown();
        try {
            Utilities.deleteDirectory(this.kafkaDir);
        }catch(FileNotFoundException fnfe){
            fnfe.printStackTrace();
        }
    }



    public static void main(String[] args){
        MiniKafkaBroker mk = new MiniKafkaBroker();
        mk.startBroker();

        String con = mk.getConnectionString();
        System.out.println(con);
        mk.shutDown();

    }





}
