package com.github.abhishek.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    Integer partkey = 0;

    // create slf4j logger
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    // creating default constructor
    public TwitterProducer(){/*  constructer  */}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        // kafka properties
        final String bootstrapServer = "127.0.0.1:9092";
        final String topic = "twitter_tweets";
        final Integer partitions = 1;

        //
        // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        // create a term list (twitter search keywords list)
        List<String> terms = Lists.newArrayList("Kafka", "java8");

        // create twitter client
        Client client = createTwitterClient(msgQueue, terms);
        client.connect();


        // Setup kafka producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true"); // Idempotent Producer
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // high throughput producer settings
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); // batch size

        // create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("STOPPING APPLICATION...");
            logger.info("Shutting down horsebird twitter client");
            client.stop();
            logger.info("Closing Kafka producer connection...");
            kafkaProducer.close();
            logger.info("Application stopped!");
        }));

        //loop to send tweets to kafka
        while(!client.isDone()){
            String tweet = null;
            try {
                tweet = msgQueue.take().toString();
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
                kafkaProducer.close();
            }
            if(tweet != null){
                logger.info(tweet);
                kafkaProducer.send(new ProducerRecord<>(topic,String.valueOf(partkey++ % partitions), tweet), new Callback(){

                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            logger.error("[Kafka-ERROR]:\t"+e);
                        }
                    }
                });
            }
        }
        logger.info("bye");

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue,List<String> terms){
        // define auth keys
        final String consumerKey = "Twr1E97gPG9lizAlBbLOvkQfG";
        final String consumerSecret = "2WlgIrDIDLQ5yX7ZlXZUwxK4pRvURYjuWsdpqbte93zeQy0ODc";
        final String token = "3536992812-dgY3uaS1ohEGOC7cTfylMIXcOMq2DZ56KV2g0np";
        final String secret = "1sPuhapCwhwCK40Wb2A6IBUjioQDsMJhknWBlPafM7ttK";

//        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(1000);

        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);

        // hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Twitter-HB-Client-01")  // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
//                .eventMessageQueue(eventQueue); // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        // hosebirdClient.connect();
        return hosebirdClient;

    }


}
