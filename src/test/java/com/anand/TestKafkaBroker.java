package com.anand;

import junit.framework.TestCase;
import kafka.server.KafkaServerStartable;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by anand.ranganathan on 9/17/15.
 */
public class TestKafkaBroker extends TestCase {


    @Test
    public void testKafkaBroker() {


        try {

            MiniKafkaBroker kafka = new MiniKafkaBroker();

            kafka.startBroker();
            assertEquals(kafka.getConnectionString(),"localhost:6667");
            kafka.shutDown();

        }  catch (Exception e){
            e.printStackTrace(System.out);
            fail("Error running local Kafka broker");
            e.printStackTrace(System.out);
        }

    }

}
