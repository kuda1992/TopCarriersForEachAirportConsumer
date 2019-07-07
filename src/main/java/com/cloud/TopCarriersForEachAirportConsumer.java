package com.cloud;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.Collections;

public class TopCarriersForEachAirportConsumer {

    private final static Logger LOGGER = Logger.getLogger(TopCarriersForEachAirportConsumer.class);

    private DynamoDB dynamoDB;

    public TopCarriersForEachAirportConsumer(KafkaConsumerClient kafkaConsumerClient, DynamoDBClient dynamoDBClient) {

        KafkaConsumer<Long, String> kafkaConsumer = kafkaConsumerClient.consumer;
        dynamoDB = dynamoDBClient.dynamoDB;

        String topic = "top-carriers-for-each-airport";
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        LOGGER.info("Listening to records on topic: " + topic);

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = kafkaConsumer.poll(1000);
            consumerRecords.forEach(this::sendTopicRecordToDynamoDB);
            kafkaConsumer.commitAsync();
        }


    }

    private void sendTopicRecordToDynamoDB(ConsumerRecord<Long, String> consumerRecord) {
        LOGGER.info("Record key: " + consumerRecord.key());
        LOGGER.info("Record value: " + consumerRecord.value());
        LOGGER.info("Record partition: " + consumerRecord.partition());
        LOGGER.info("Record offset: " + consumerRecord.offset());

        final String[] carrierAndDelayAverage = consumerRecord.value()
                .replace("(", "")
                .replace("[", "")
                .replace("]", "")
                .replace(")", "")
                .replace("\"", "")
                .replace("'", "")
                .split(",");

        if (carrierAndDelayAverage.length == 2) {
            final String carrier = carrierAndDelayAverage[0];
            final Float averageDelay = Float.parseFloat(carrierAndDelayAverage[1]);
            LOGGER.info("Sending carrier: " + carrier + " with average delay " + averageDelay);

            Table table = dynamoDB.getTable("top-carriers-for-each-airport-streaming");

            try {
                final Item item = new Item()
                        .withPrimaryKey("average_delay", averageDelay)
                        .with("carrier", carrier);

                final PutItemOutcome putItemOutcome = table.putItem(item);
                LOGGER.info("Item has been put into database successfully" + putItemOutcome.getPutItemResult());

            } catch (Exception e) {
                LOGGER.error("Failed to put item into table");
                e.printStackTrace();
            }

        }
    }


}
