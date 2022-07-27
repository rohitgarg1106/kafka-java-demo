package io.conduktor.demos;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {

        log.info("Producer Demo Keys");

        //create Producer Properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        for (int i=0;i<10;i++){

            //create Producer
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
            String topic = "secondTopic";
            String key = "key" + i;
            String value = "version" + i;
            //create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            //send the data - asynchronous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null){
                        log.error("Error while producing: ", e);

                    }
                    else{
                        log.info("Received new metadata/ \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Key: " + producerRecord.key() + "\n" +
                                "Value: " + producerRecord.value() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    }
                }
            });

            try{
                Thread.sleep(1000);
            }
            catch (Exception e){
                e.printStackTrace();
            }

            //flush the producer
            producer.flush();

            //flush and close producer
            producer.close();
        }





    }
}
