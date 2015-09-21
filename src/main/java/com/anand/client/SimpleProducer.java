package com.anand.client;

import com.anand.MiniKafkaBroker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;


/**
 * Created by anand.ranganathan on 9/20/15.
 */
public class SimpleProducer {

    String topic=null;

    public SimpleProducer(String topicName){
        this.topic= topicName;
    }


 public void loadProducerData(String connString) throws ExecutionException, InterruptedException {
     //    long events = Long.parseLong(args[0]);
     long events=10;
     Random rnd = new Random();
     Properties props = new Properties();

     props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,connString);
     props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
     props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

     KafkaProducer<String,String> producer = new KafkaProducer<String,String>(props);

     boolean sync = false;
  //   String topic="mytopic1";
     String key = "mykey";
     String value = "myvalue";
     ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic, key, value);



     for (int i=0;i<events;i++){
         long runtime = new Date().getTime();
         String keymsg = Integer.toString(i);
         String messageStr = "message "+keymsg;
         System.out.println(" producer writing "+ messageStr);

         producer.send(new ProducerRecord<String,String>(topic,keymsg,messageStr)).get();
     }

     producer.close();
  //   System.exit(0);
 }



    public void testKafkaBroker() {
        try {
            MiniKafkaBroker kafka = new MiniKafkaBroker();
            kafka.startBroker();
            loadProducerData(kafka.getConnectionString());
            kafka.shutDown();

        }  catch (Exception e){
            e.printStackTrace(System.out);
            e.printStackTrace(System.out);
        }

    }


    public static void main(String[] args){
        SimpleProducer sp  = new SimpleProducer("mytopic");
        sp.testKafkaBroker();
    }
}
