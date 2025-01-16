package com.tikeysoft;

import org.eclipse.paho.client.mqttv3.*;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class RawDataLogger {
    private MqttClient client;
    private PrintWriter csvWriter;

    public RawDataLogger() {
        try {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            csvWriter = new PrintWriter(new FileWriter("raw_data_logs_" + timestamp + ".csv", true));
            csvWriter.println("Timestamp,Topic,Payload"); // CSV header
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void connectAndSubscribe(String broker, String clientId, String[] topics) throws MqttException {
        client = new MqttClient(broker, clientId);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);

        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                System.out.println("Connection lost: " + cause.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                String payload = new String(message.getPayload());
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                csvWriter.println(timestamp + "," + topic + "," + payload);
                csvWriter.flush(); // Ensure data is written to the file immediately
                System.out.println("Logged " + timestamp + "," + topic + "," + payload);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Not used in this example
            }
        });

        System.out.println("Connecting to broker: " + broker);
        client.connect(connOpts);
        System.out.println("Connected");

        for (String topic : topics) {
            client.subscribe(topic);
            System.out.println("Subscribed to topic: " + topic);
        }
    }
}