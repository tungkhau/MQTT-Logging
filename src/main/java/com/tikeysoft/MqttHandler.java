package com.tikeysoft;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MqttHandler {
    private MqttClient client;
    private PrintWriter csvWriter;

    public MqttHandler() {
        try {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            csvWriter = new PrintWriter(new FileWriter("mqtt_logs_" + timestamp + ".csv", true));
            csvWriter.println("Timestamp,Topic,Converted Value,Raw Value"); // CSV header
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
                if (message.isRetained()) return;
                String payload = new String(message.getPayload());

                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(payload);
                JsonNode data = json.get("data");

                if (data != null) {
                    JsonNode payloadData = data.get("payload");

                    if (payloadData != null) {
                        payloadData.fields().forEachRemaining(entry -> {
                            if (entry.getKey().contains("/iolinkmaster/port")) {
                                JsonNode portData = entry.getValue();
                                String hexString = portData.get("data").asText();
                                try {
                                    convertData(topic, hexString);
                                } catch (MqttException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
                    } else {
                        System.out.println("Payload data is null");
                    }
                } else {
                    System.out.println("Data is null");
                }
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

    private void convertData(String topic, String hexString) throws MqttException {
        String convertedValue = switch (topic) {
            // Signal
            case "AE_01/signal/heart_beat" -> String.valueOf(MqttDataConverter.convertHeartBeat(hexString));
            case "AE_01/signal/cutter" -> String.valueOf(MqttDataConverter.convertCutter(hexString));
            case "AE_01/signal/billet_cutting" -> String.valueOf(MqttDataConverter.convertBilletCutting(hexString));

            // Condition
            case "AE_01/condition/die_temp" -> String.valueOf(MqttDataConverter.convertDieTemp(hexString));
            case "AE_01/condition/billet_temp" -> String.valueOf(MqttDataConverter.convertBilletTemp(hexString));
            case "AE_01/condition/ramp_pressure" -> String.valueOf(MqttDataConverter.convertRampPressure(hexString));

            // Production
            case "AE_01/production/billet" -> String.valueOf(MqttDataConverter.convertBillet(hexString));
            case "AE_01/production/billet_waste" -> String.valueOf(MqttDataConverter.convertBilletWaste(hexString));
            case "AE_01/production/semi_profile_A" -> String.valueOf(MqttDataConverter.convertSemiProfileA(hexString));
            case "AE_01/production/semi_profile_B" -> String.valueOf(MqttDataConverter.convertSemiProfileB(hexString));

            // Process Billet value
            case "AE_01/signal/billet_detecting" -> String.valueOf(MqttDataConverter.convertBilletDetecting(hexString));

            // Process Billet waste value
            case "AE_01/signal/end" -> String.valueOf(MqttDataConverter.convertSignalEnd(hexString));

            // Process Semi profile value
            case "AE_01/signal/puller_A" -> String.valueOf(MqttDataConverter.convertPullerA(hexString));
            case "AE_01/signal/puller_B" -> String.valueOf(MqttDataConverter.convertPullerB(hexString));
            default -> throw new IllegalArgumentException("Unknown topic: " + topic);
        };
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        csvWriter.println(timestamp + "," + topic + "," + convertedValue + "," + hexString);
        csvWriter.flush(); // Ensure data is written to the file immediately
    }}