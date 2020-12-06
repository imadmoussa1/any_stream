package mystream.twitter;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

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
