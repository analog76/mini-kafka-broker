package com.anand.client;

import com.anand.MiniKafkaBroker;
import com.anand.properties.KafkaProperties;

import java.util.concurrent.ExecutionException;

/**
 * Created by anand.ranganathan on 9/20/15.
 */
public class ProducerConsumerDemo {

    public static void main(String[] args){
        ProducerConsumerDemo pcd = new ProducerConsumerDemo();
        pcd.execute();
    }

    public void execute(){
        MiniKafkaBroker kafka = new MiniKafkaBroker();
        kafka.startBroker();

        String topicName="test-topic";


        SimpleProducer sp   = new SimpleProducer(topicName);


        try {
            sp.loadProducerData(kafka.getConnectionString());
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



        SimpleConsumer sc = new SimpleConsumer(kafka.getZKConnectionString(),topicName);
        sc.start();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

           kafka.shutDown();

    }
}
