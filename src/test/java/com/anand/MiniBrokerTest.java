package com.anand;

import com.anand.client.SimpleConsumer;
import com.anand.client.SimpleProducer;
import junit.framework.TestCase;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.*;

import static org.junit.Assert.*;


import java.io.IOException;

/**
 * Created by anand.ranganathan on 9/17/15.
 */
public class MiniBrokerTest {
    static MiniKafkaBroker kafka  = null;


    @BeforeClass
    public static void setUp(){
        kafka = new MiniKafkaBroker();
        kafka.startBroker();

    }

    @Test
    public void testProducer() {
        try {
             SimpleProducer sp  =new SimpleProducer("topic-test");
            sp.loadProducerData(kafka.getConnectionString());

            assertEquals(kafka.getConnectionString(), "localhost:6667");


        }  catch (Exception e){
            e.printStackTrace(System.out);
            fail("Error running local Kafka broker");
            e.printStackTrace(System.out);
        }

    }

    @Test
    public void testConsumer() {
        try {
            SimpleConsumer sp  =new SimpleConsumer(kafka.getZKConnectionString(),"topic-test");
            sp.start();

            assertEquals(kafka.getZKConnectionString(), "localhost:2181");


        }  catch (Exception e){
            e.printStackTrace(System.out);
            fail("Error running local Kafka broker");
            e.printStackTrace(System.out);
        }

    }


    @AfterClass
    public static  void tearDown() throws InterruptedException {
        Thread.sleep(10000);
        kafka.shutDown();
    }


}
