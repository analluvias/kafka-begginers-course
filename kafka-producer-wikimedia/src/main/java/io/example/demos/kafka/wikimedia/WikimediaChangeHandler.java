package io.example.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    //essa classe terá um produtor e um tópico para o qual enviará as mensagens
    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic){
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        //nothing here
    }

    @Override
    public void onClosed(){
        //fechando o producer
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {

        log.info(messageEvent.getData());

        //quando a stream recebe uma mensagem, vamos usar um código assincrono para o producer
        //mandando uma mensagem para este tópico
        kafkaProducer.send(new ProducerRecord<String, String>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String comment) throws Exception {
        //nothing here
    }

    @Override
    public void onError(Throwable t) {
        //em caso de erro, vamos só mandar um log
        log.error("Error in Stream Reading", t);
    }
}
