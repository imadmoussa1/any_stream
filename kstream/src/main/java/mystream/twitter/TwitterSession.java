package mystream.twitter;

import mystream.KafkaProperties;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class TwitterSession {
  private final String bearerToken;
  private String searchString;
  private String sessionType;
  private String tweetFields = "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,withheld";
  private String expansions = "author_id,referenced_tweets.id,in_reply_to_user_id,attachments.media_keys,geo.place_id,entities.mentions.username,referenced_tweets.id.author_id";
  private String userFields = "created_at,description,location,profile_image_url,protected,public_metrics,verified";
  private String placeFields = "country,country_code,geo,place_type";
  private String mediaFields = "preview_image_url,public_metrics,width,height";


  public TwitterSession() {
    this.bearerToken = KafkaProperties.bearerToken;
    this.sessionType = "SampleStream";
//    this.sessionType = "FilterStream";
  }

  public TwitterSession(String searchString) {
    this.bearerToken = KafkaProperties.bearerToken;
    this.sessionType = "Search";
    this.searchString = searchString;
  }

  private HttpClient getHttpClient() {
    HttpClient httpClient = HttpClients.custom()
            .setDefaultRequestConfig(RequestConfig.custom()
                    .setCookieSpec(CookieSpecs.STANDARD).build())
            .build();
    return httpClient;
  }

  public HttpEntity connectStream() throws URISyntaxException, IOException {
    URIBuilder uriBuilder = new URIBuilder();
    ArrayList<NameValuePair> queryParameters;
    queryParameters = new ArrayList<>();
    queryParameters.add(new BasicNameValuePair("tweet.fields", tweetFields));
    queryParameters.add(new BasicNameValuePair("expansions", expansions));
    queryParameters.add(new BasicNameValuePair("user.fields", userFields));
    queryParameters.add(new BasicNameValuePair("place.fields", placeFields));
    queryParameters.add(new BasicNameValuePair("media.fields", mediaFields));

    if (this.sessionType == "SampleStream") {
      uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream");
    }
    if (this.sessionType == "FilterStream") {
      uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream");
    }
    if (this.sessionType == "Search") {
      uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/recent");
      queryParameters.add(new BasicNameValuePair("query", this.searchString));
    }
    uriBuilder.addParameters(queryParameters);
    HttpGet httpGet = new HttpGet(uriBuilder.build());
    httpGet.setHeader("Authorization", String.format("Bearer %s", this.bearerToken));
    httpGet.setHeader("Content-Type", "application/json");

    HttpResponse response = this.getHttpClient().execute(httpGet);
    HttpEntity entity = response.getEntity();
    return entity;
  }

  /*
   * Helper method to setup rules before streaming data
   * */
  private void setupRules(Map<String, String> rules) throws IOException, URISyntaxException {
    List<String> existingRules = this.getRules();
    if (existingRules.size() > 0) {
      this.deleteRules(existingRules);
    }
    createRules(rules);
  }

  /*
   * Helper method to create rules for filtering
   * */
  private void createRules(Map<String, String> rules) throws URISyntaxException, IOException {

    URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");
    HttpPost httpPost = new HttpPost(uriBuilder.build());
    httpPost.setHeader("Authorization", String.format("Bearer %s", this.bearerToken));
    httpPost.setHeader("content-type", "application/json");
    StringEntity body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules));
    httpPost.setEntity(body);
    HttpResponse response = this.getHttpClient().execute(httpPost);
    HttpEntity entity = response.getEntity();
    if (null != entity) {
      System.out.println(EntityUtils.toString(entity, "UTF-8"));
    }
  }

  /*
   * Helper method to get existing rules
   * */
  private List<String> getRules() throws URISyntaxException, IOException {
    List<String> rules = new ArrayList<>();

    URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

    HttpGet httpGet = new HttpGet(uriBuilder.build());
    httpGet.setHeader("Authorization", String.format("Bearer %s", this.bearerToken));
    httpGet.setHeader("content-type", "application/json");
    HttpResponse response = this.getHttpClient().execute(httpGet);
    HttpEntity entity = response.getEntity();
    if (null != entity) {
      JSONObject json = new JSONObject(EntityUtils.toString(entity, "UTF-8"));
      if (json.length() > 1) {
        JSONArray array = (JSONArray) json.get("data");
        for (int i = 0; i < array.length(); i++) {
          JSONObject jsonObject = (JSONObject) array.get(i);
          rules.add(jsonObject.getString("id"));
        }
      }
    }
    return rules;
  }

  /*
   * Helper method to delete rules
   * */
  private void deleteRules(List<String> existingRules) throws URISyntaxException, IOException {

    URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

    HttpPost httpPost = new HttpPost(uriBuilder.build());
    httpPost.setHeader("Authorization", String.format("Bearer %s", this.bearerToken));
    httpPost.setHeader("content-type", "application/json");
    StringEntity body = new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules));
    httpPost.setEntity(body);
    HttpResponse response = this.getHttpClient().execute(httpPost);
    HttpEntity entity = response.getEntity();
    if (null != entity) {
      System.out.println(EntityUtils.toString(entity, "UTF-8"));
    }
  }

  private String getFormattedString(String string, List<String> ids) {
    StringBuilder sb = new StringBuilder();
    if (ids.size() == 1) {
      return String.format(string, "\"" + ids.get(0) + "\"");
    } else {
      for (String id : ids) {
        sb.append("\"" + id + "\"" + ",");
      }
      String result = sb.toString();
      return String.format(string, result.substring(0, result.length() - 1));
    }
  }

  private String getFormattedString(String string, Map<String, String> rules) {
    StringBuilder sb = new StringBuilder();
    if (rules.size() == 1) {
      String key = rules.keySet().iterator().next();
      return String.format(string, "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}");
    } else {
      for (Map.Entry<String, String> entry : rules.entrySet()) {
        String value = entry.getKey();
        String tag = entry.getValue();
        sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",");
      }
      String result = sb.toString();
      return String.format(string, result.substring(0, result.length() - 1));
    }
  }
}