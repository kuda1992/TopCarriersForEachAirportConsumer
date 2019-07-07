package com.cloud;

import org.apache.log4j.Logger;

public class App {

    private final static Logger LOGGER = Logger.getLogger(App.class);

    public static void main(String[] args) {

        if (args.length < 3) {
            LOGGER.error("Please supply all arguments to run the progam");
            return;
        }

        String awsKeyId = args[0];
        String awsAccessKey = args[1];
        String host = args[2];
        String clientId = args[3];
        String groupId = args[4];


        final DynamoDBClient dynamoDBClient = new DynamoDBClient(awsKeyId, awsAccessKey);

        final TopCarriersForEachAirportConsumer topCarriersForEachAirportConsumer = new TopCarriersForEachAirportConsumer(new KafkaConsumerClient(host, clientId, groupId), dynamoDBClient);


    }
}
