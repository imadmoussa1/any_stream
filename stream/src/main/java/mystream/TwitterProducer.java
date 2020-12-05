package mystream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.json.JSONObject;

public class TwitterProducer extends Thread {
  private final KafkaProducer<String, String> producer;
  private final String topic;
  private final Boolean isAsync;
  private final CountDownLatch latch;

  public TwitterProducer(final String topic, final Boolean isAsync, final String transactionalId,
      final boolean enableIdempotency, final int transactionTimeoutMs, final CountDownLatch latch) {

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "TwitterProducer");
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
  }

  KafkaProducer<String, String> get() {
    return producer;
  }

  @Override
  public void run() {
    int recordsSent = 0;

    String bearerToken = "AAAAAAAAAAAAAAAAAAAAAAkpBAEAAAAAKrEty3ZPbac0Yg8sfXiFnWeR7%2FY%3DX2HRep9eyBFwYojEW2n2XTj5ziNDc9kKsTz9eQW4fGihBc0xfU";

    HttpClient httpClient = HttpClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()).build();

    try {
      URIBuilder uriBuilder  = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream");
      HttpGet httpGet = new HttpGet(uriBuilder.build());
      httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));

      HttpResponse response = httpClient.execute(httpGet);
      HttpEntity entity = response.getEntity();
      if (null != entity) {
        BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
        String tweetData = reader.readLine();
        while (tweetData != null) {
          JSONObject tweetObj = new JSONObject(tweetData);
          JSONObject data = tweetObj.getJSONObject("data");
          String id = data.getString("id");
          String tweet = data.getString("text");
          System.out.println(tweet);
          long startTime = System.currentTimeMillis();
          if (isAsync) { // Send asynchronously
            producer.send(new ProducerRecord<>(topic, id, tweet), new TweetCallBack(startTime, id, tweet));
          } else {
            try {
              producer.send(new ProducerRecord<>(topic, id, tweet)).get();
              System.out.println("Sent message: (" + id + ", " + tweet + ")");
            } catch (InterruptedException | ExecutionException e) {
              e.printStackTrace();
            }
          }
          recordsSent += 1;
          tweetData = reader.readLine();
        }
        System.out.println("Producer sent " + recordsSent + " records successfully");
        latch.countDown();
      }
    } catch (URISyntaxException e) {
      e.printStackTrace();
    } catch (ClientProtocolException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }


  }
}

class TweetCallBack implements Callback {

  private final long startTime;
  private final String id;
  private final String tweet;

  public TweetCallBack(long startTime, String id, String tweet) {
    this.startTime = startTime;
    this.id = id;
    this.tweet = tweet;
  }

  /**
   * A callback method the user can implement to provide asynchronous handling of
   * request completion. This method will be called when the record sent to the
   * server has been acknowledged. When exception is not null in the callback,
   * metadata will contain the special -1 value for all fields except for
   * topicPartition, which will be valid.
   *
   * @param metadata  The metadata for the record that was sent (i.e. the
   *                  partition and offset). An empty metadata with -1 value for
   *                  all fields except for topicPartition will be returned if an
   *                  error occurred.
   * @param exception The exception thrown during processing of this record. Null
   *                  if no error occurred.
   */
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    long elapsedTime = System.currentTimeMillis() - startTime;
    if (metadata != null) {
      System.out.println("message(" + id + ", " + tweet + ") sent to partition(" + metadata.partition() + "), "
          + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
    } else {
      exception.printStackTrace();
    }
  }
}
