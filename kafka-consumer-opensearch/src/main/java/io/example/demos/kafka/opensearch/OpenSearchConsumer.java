package io.example.demos.kafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //create an opensearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create our kafka client

        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        //we need to create the index on the OpenSearch if it does not exist already
        //tirar dos comentários e rodas da primeira vez

//        try(openSearchClient; consumer) {
//
//            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
//            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
///           log.info("The wikimedia index has been created.");
//
//        }catch (OpenSearchStatusException e){
//            log.info("The wikimedia index already exists.");
//        }

        //subscribe the consumer
        consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

       while (true){

           ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

           int recordCount = records.count();
           log.info("Received " + recordCount + " records.");

           for (ConsumerRecord<String, String> record: records){
               //send the record into openSearch

               //strategy 1
               //define um id usando kafka coordinates
               String id = record.topic() + "_" + record.partition() + "_" + record.offset();

               IndexRequest indexRequest = new IndexRequest("wikimedia")
                       .source(record.value(), XContentType.JSON)
                       .id(id);

               IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

               log.info(response.getId());
           }
       }

    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "consumer-opersearch-demo";

        //create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //create consumer
        return new KafkaConsumer<>(properties);
    }

}
