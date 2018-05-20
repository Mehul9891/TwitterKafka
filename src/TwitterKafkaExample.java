import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterKafkaExample {

    public static void main(String [] args) throws Exception{
        {
            LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);


            String consumerKey = "BfdfcfOvGktY0D4CUaGUuvjcc";
            String consumerSecret = "LCcSGzXbKYkSoV6CdUSWNaK4UoJLdVrGr3fTIKJyVsXyif71l5";
            String accessToken = "3195562729-eVZdGWISNLba7ybD1T6FcGZnX2vK5XDAfL4XpGg";
            String accessTokenSecret = "RDgJJJy4yAqeIds08RtpMtng9j4peIOOwwIP86sDz0etB";
            String topicName = "my_latest_topic";
            Scanner scanner = new Scanner(System.in);
            String[] keyWords = scanner.nextLine().split(" ");

            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(accessToken)
                    .setOAuthAccessTokenSecret(accessTokenSecret);

            TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
            StatusListener listener = new StatusListener() {

                @Override
                public void onStatus(Status status) {
                    queue.offer(status);

                    System.out.println("@" + status.getUser().getScreenName()
                            + " - " + status.getText());
                    System.out.println("@" + status.getUser().getScreenName());

                    for(URLEntity urle : status.getURLEntities()) {
                        System.out.println(urle.getDisplayURL());
                    }

                    for(HashtagEntity hashtage : status.getHashtagEntities()) {
                        System.out.println(hashtage.getText());
                    }
                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    System.out.println("Got a status deletion notice id:"
                            + statusDeletionNotice.getStatusId());
                }

                @Override
                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                    System.out.println("Got track limitation notice:" +
                            numberOfLimitedStatuses);
                }

                @Override
                public void onScrubGeo(long userId, long upToStatusId) {
                    System.out.println("Got scrub_geo event userId:" + userId +
                            "upToStatusId:" + upToStatusId);
                }

                @Override
                public void onStallWarning(StallWarning warning) {
                    System.out.println("Got stall warning:" + warning);
                }

                @Override
                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
            };
            twitterStream.addListener(listener);

            FilterQuery query = new FilterQuery().track(keyWords);
            twitterStream.filter(query);

            Thread.sleep(5000);

            //Add Kafka producer config settings
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9094");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);

            props.put("key.serializer",
                    "org.apache.kafka.common.serializa-tion.StringSerializer");
            props.put("value.serializer",
                    "org.apache.kafka.common.serializa-tion.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<String, String>(props);
            int i = 0;
            int j = 0;

            while(i < 10) {
                Status ret = queue.poll();

                if (ret == null) {
                    Thread.sleep(100);
                    i++;
                }else {
                    for(HashtagEntity hashtage : ret.getHashtagEntities()) {
                        System.out.println("Hashtag: " + hashtage.getText());
                        producer.send(new ProducerRecord<String, String>(
                                topicName, Integer.toString(j++), hashtage.getText()));
                    }
                }
            }
            producer.close();
            Thread.sleep(5000);
            twitterStream.shutdown();
        }
    }
}
