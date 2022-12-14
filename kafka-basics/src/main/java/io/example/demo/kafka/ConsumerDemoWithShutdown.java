package io.example.demo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a kafka consumer");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-third-application"; //ID DO GRUPO DE CONSUMERS
        String topic = "demo_j1";

        //create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //pode ser none/ earliest / latest -> none - não ler/ earliest - ler do começo do topico / latest - ler so a partir de agora
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName()); //esse atributo serve para settar o modo de organização a cada vez que entra ou sai um consumer novo
        //essa configuração CooperativeStickyAssignor é uma das MELHORES, estudar melhor as opções

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the current thred
        final Thread mainThread = Thread.currentThread();

        //adicionando o gancho de desligamento
        Runtime.getRuntime().addShutdownHook(new Thread(){

            public void run(){

                //quando detectar um desligamento, vai lançar um consumer.wakeup que manda uma exceção
                //para desligamento do poll abaixo (dentro do while true)
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                //voltando novamente à thread main
                try {

                    mainThread.join();

                } catch (InterruptedException e) {

                    e.printStackTrace();

                }

            }
        });

        try{

            //subscribe consumer to our topics - apenas o nosso tópico demo_j1 (nesse caso), mas podem ser vários
            consumer.subscribe(Collections.singletonList(topic));

            //captação de novos dados
            while (true){
                //-> vá ao kafka e capture o dado agora, se não vier, espere 1000milisegundos
                //se não vier novamente, então teremos uma coleção vazia
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records){
                    log.info("Key: "+record.key() + ", Value: "+ record.value());
                    log.info("Partition: "+ record.partition() + ", Offeset: "+ record.offset());
                }
            }

        }catch (WakeupException e){

            //exceção esperada
            log.info("Wake up exception!");

        }catch (Exception e){

            log.error("Unexpected exception");

        }finally {

            consumer.close(); //também vai commitar os offsets, se necessário
            log.info("The consumer is now gracefully closed");

        }



    }
}
