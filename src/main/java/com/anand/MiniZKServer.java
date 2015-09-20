package com.anand;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Properties;

import com.anand.properties.ZookeeperProperties;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/**
 * Created by anand.ranganathan on 9/17/15.
 */
public class MiniZKServer implements ZookeeperProperties {

    ZooKeeperServerMain zooKeeperServer;
    private ServerCnxnFactory factory;
    File snapshotDir  = null;
    File logDir = null;
    int port =-1;



    public void startZookeeper(Properties zkProperties) throws IOException{


        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(zkProperties);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);





        new Thread() {
            public void run() {
                try {
                    zooKeeperServer.runFromConfig(configuration);
                } catch (IOException e) {
                    System.out.println("ZooKeeper Failed");
                    e.printStackTrace(System.err);
                }
            }
        }.start();
    }


    public int getPort(){
        return this.port;
    }

    public int assignPort() throws IllegalStateException, IOException{

        try {
            ServerSocket s = new ServerSocket(DEFAULT_PORT);
        }catch(IllegalStateException ioe){
            throw new IllegalStateException(" couldn't get the available port:"+ ioe.getMessage(),ioe);
        }catch(IOException ioe){
            throw new IOException(" couldn't get the available port:"+ ioe.getMessage(),ioe);

        }

        this.port=DEFAULT_PORT;

        return  this.port;
    }



    public void startZookeeper() throws IOException{
         this.port = assignPort();

        this.factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), this.MAX_CONNECTION);
        snapshotDir   = Utilities.getDirectory("mini-zk/snapshot");
        logDir = Utilities.getDirectory("mini-zk/log");

        try {
            factory.startup(new ZooKeeperServer(snapshotDir, logDir, 400));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }


    public void startZookeeper(int cluster) throws IOException{
        if (this.port == -1) {
            this.port = assignPort();
        }
        this.factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), this.MAX_CONNECTION);
        File  snapshotDir   = Utilities.getDirectory("mini-zk/snapshot");
        File logDir = Utilities.getDirectory("mini-zk/log");

        try {
            factory.startup(new ZooKeeperServer(snapshotDir, logDir, 400));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }







    public String getConnectionString(){
        return "localhost:"+this.getPort();
    }



    public void shutdown() {
        System.out.println("zookeeper shutdown beginning for "+this.getConnectionString());

        factory.shutdown();

        try {
            Utilities.deleteDirectory(snapshotDir);
            Utilities.deleteDirectory(logDir);
        }catch(FileNotFoundException ioe){
            ioe.printStackTrace();;
        }catch(IOException ioe){
            ioe.printStackTrace();;
        }

        System.out.println("zookeeper shutdown completed for  "+this.getConnectionString());

    }





    public static void main(String[] args){
        MiniZKServer ez = new MiniZKServer();
        try {
            ez.startZookeeper();
            int port = ez.port;
            System.out.println(" zookeeper connections are localhost:" + port);
            ez.shutdown();

        }catch(IOException ioe){
            ioe.printStackTrace();
        }

    }




}
