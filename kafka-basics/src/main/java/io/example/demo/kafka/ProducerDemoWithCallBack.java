package io.example.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {


    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a kafka producer");

        //criando producer properties

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //criando o producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);



        for (int i = 0; i < 10; i++){

            //criando o valor que eu quero enviar para o meu tópico "demo_java"

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_j1", "hello world" + i);

            //enviando dados - operação assincrona

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //executa sempre que um dado eh enviado com sucesso ou que uma excessão for lançada

                    if (e == null){
                        // o dado foi enviado com sucesso
                        log.info("Received new metadata /" +
                                "\nTopic: " + metadata.topic() +
                                "\nPartition: " + metadata.partition() +
                                "\nOffset: "+ metadata.offset() +
                                "\nTimestamp: " + metadata.timestamp());
                    }else {
                        log.error("Error while producing", e);
                    }
                }
            });

            try {
                Thread.sleep(1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }



        //flush e fechar o producer

        //////////flush - fique nessa linha de código até que todas as informações tenham sido enviadas
        producer.flush();

        /////////close producer
        producer.close();
    }
}
