package mystream.twitter;

import mystream.KafkaProperties;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class TwitterProducer extends Thread {
  private final KafkaProducer<String, String> producer;
  private final String topic;
  private final Boolean isAsync;
  private final CountDownLatch latch;
  private String twitterSearchQuery;

  public TwitterProducer(final String topic, final Boolean isAsync, final String transactionalId,
                         final boolean enableIdempotency, final int transactionTimeoutMs, final CountDownLatch latch,
                         String twitterSearchQuery) {

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "SampledStream");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    if (transactionTimeoutMs > 0) {
      props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
    }
    if (transactionalId != null) {
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    }
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotency);

    producer = new KafkaProducer<>(props);
    this.topic = topic;
    this.isAsync = isAsync;
    this.latch = latch;
    this.twitterSearchQuery = twitterSearchQuery;
  }

  KafkaProducer<String, String> get() {
    return producer;
  }

  @Override
  public void run() {
    int recordsSent = 0;

    try {
      TwitterSession ts = new TwitterSession();
      HttpEntity entity = ts.connectStream();
      if (null != entity) {
        if (this.twitterSearchQuery == null) {
          BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
          String tweetData = reader.readLine();
          while (tweetData != null) {
            this.parseTweet(tweetData);
            recordsSent += 1;
            tweetData = reader.readLine();
          }
        } else {
          String searchResponse = EntityUtils.toString(entity, "UTF-8");
          System.out.println(searchResponse);
        }
      }
      System.out.println("Producer sent " + recordsSent + " records successfully");
      latch.countDown();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  private void parseTweet(String tweetData) {
    JSONObject tweetObj = new JSONObject(tweetData);
    String id = "";
    if (tweetObj.has("errors")) {
      System.out.println(tweetData);
    } else {
      if (tweetObj.has("data")) {
        JSONObject data = tweetObj.getJSONObject("data");
        id = data.getString("id");
//        String tweet = data.getString("text");
//        System.out.println(tweet);
      }
    }
    long startTime = System.currentTimeMillis();
    if (isAsync) { // Send asynchronously
      producer.send(new ProducerRecord<>(topic, id, tweetData), new TweetCallBack(startTime, id, tweetData));
    } else {
      try {
        producer.send(new ProducerRecord<>(topic, id, tweetData)).get();
        System.out.println("Sent message: (" + id + ", " + tweetData + ")");
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }
}
