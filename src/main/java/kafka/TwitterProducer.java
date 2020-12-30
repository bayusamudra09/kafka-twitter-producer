package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer   {
    //creat mathod to grep the token from file
    static String ReadFile(String x) throws FileNotFoundException {
        File filepath = new File("token file path");
        Scanner s = new Scanner(filepath);
        ArrayList<String> list = new ArrayList<>();
        HashMap<String, String> dict = new HashMap<>();

        while (s.hasNextLine()){
            String[] parts = s.nextLine().split(":");
            dict.put(parts[0], parts[1]);
        }
        return dict.get(x);
    }

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey = ReadFile("Api key");
    String consumerSecret = ReadFile("Api key secret");
    String token = ReadFile("access token");
    String tokenSecret = ReadFile("access token secret");

    List<String> terms = Lists.newArrayList("covid", "vaksin"); //set words you want to search in twitter

    //build constructure
    public TwitterProducer() throws FileNotFoundException {}

    public static void main(String[] args) throws FileNotFoundException {
        new TwitterProducer().run();

    }
    public void run(){
        logger.info("Start setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //create twitter client
        Client client = twitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        //create a kafka producer
        KafkaProducer<String, String> producer= kafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping aplication ...");
            client.stop();
            logger.info("closing producer ...");
            producer.close();
            logger.info("done !");
        }));

        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>("your-topic", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Check your producer", e); //Throw an error
                        }
                    }
                });
            }
        }
        logger.info("End of Application !!!");
    }

    public Client twitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
//                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
    //create kafka producer
    public KafkaProducer<String, String> kafkaProducer(){
        String server = "localhost:9092";
        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe producer
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //set acks to all
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //ensure there is no data duplicate
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        //compression batch
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //set small delay
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //set batch size to 32kb

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
