package com.anand;

import com.anand.properties.KafkaProperties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Created by anand.ranganathan on 9/17/15.
 */
public class MiniKafkaBroker implements KafkaProperties {

    KafkaServer broker = null;

    private File kafkaDir= null;
    private MiniZKServer zk = null;
    Properties kafkaProperites = new Properties();
    int port = -1;


    public MiniKafkaBroker( ){
            loadProperties();
    }

    public MiniKafkaBroker(boolean dynamic){
        if(dynamic){
            loadProperties(true);
        }else {
            loadProperties();
        }
    }

    public MiniKafkaBroker(Properties baseProperties){
        loadProperties();
        baseProperties = baseProperties;
    }




    private void loadProperties(boolean dynamicPort){
        String strPort=null;
        try {
            this.port = Utilities.assignPort();
            strPort = Integer.toString(port);
         }catch(IOException ioe){
            ioe.printStackTrace();
        }
        kafkaDir = getDirectory("/mini-kafka/kafka-logs");
        kafkaProperites = new Properties();

        kafkaProperites.setProperty("hostname", this.HOSTNAME);
        kafkaProperites.setProperty("port", strPort);
        kafkaProperites.setProperty("broker.id", this.BROKER_ID);
        kafkaProperites.setProperty("log.dir", kafkaDir.getAbsolutePath());
        kafkaProperites.setProperty("enable.zookeeper", "false");
        kafkaProperites.setProperty("zookeeper.connect",getZkConnection());
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


    public String getZKConnectionString(){
        return this.zk.getConnectionString();
    }



    public void startBroker() {
            startBroker(kafkaProperites);
    }

    private void startBroker(Properties props) {
        broker = new KafkaServer(new KafkaConfig(props), new SystemTime());
        broker.startup();
        System.out.println(" Broker started at "+this.getConnectionString());
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
        mk.getZKConnectionString();
        System.out.println(con);
        mk.loadProducerData(mk.getConnectionString());
        mk.shutDown();

    }


    public void loadProducerData(String connString)   {
        //    long events = Long.parseLong(args[0]);
        long events=10;
        Random rnd = new Random();
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,connString);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(props);

        boolean sync = false;
        String topic="mytopic1";
        String key = "mykey";
        String value = "myvalue";
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic, key, value);



        for (int i=0;i<events;i++){
            long runtime = new Date().getTime();
            String keymsg = Integer.toString(i);
            String messageStr = "message "+keymsg;

            try {
                producer.send(new ProducerRecord<String, String>(topic, keymsg, messageStr)).get();
            }catch(Exception e){
                e.printStackTrace();
            }
        }




        producer.close();
     //   System.exit(0);
    }





}
