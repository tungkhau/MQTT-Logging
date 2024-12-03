package com.tikeysoft;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Main {
    public static void main(String[] args) {
        String broker = "tcp://localhost:1883";
        String clientId = "Mqtt_logging";

        String[] topics = {
                "AE_01/condition/die_temp",
                "AE_01/condition/billet_temp",
                "AE_01/condition/ramp_pressure",
                "AE_01/production/billet",
                "AE_01/production/billet_waste",
                "AE_01/production/semi_profile_A",
                "AE_01/production/semi_profile_B",
                "AE_01/signal/start",
                "AE_01/signal/end",
                "AE_01/signal/billet_detecting",
                "AE_01/signal/heart_beat",
                "AE_01/signal/puller_A",
                "AE_01/signal/puller_B",
                "AE_01/signal/cutter"
        };

        try {
            MqttClient client = new MqttClient(broker, clientId);
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
                                    double convertedValue = convertData(topic, hexString);
                                    System.out.println("Topic: " + topic + " Extracted data: " + convertedValue);
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

        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }

    private static double convertData(String topic, String hexString) {
        switch (topic) {
            case "AE_01/condition/die_temp":
                return MqttDataConverter.convertDieTemp(hexString);
            case "AE_01/condition/billet_temp":
                return MqttDataConverter.convertBilletTemp(hexString);
            case "AE_01/condition/ramp_pressure":
                return MqttDataConverter.convertRampPressure(hexString);
            case "AE_01/production/billet":
                return MqttDataConverter.convertBillet(hexString);
            case "AE_01/production/billet_waste":
                return MqttDataConverter.convertBilletWaste(hexString);
            case "AE_01/production/semi_profile_A":
                return MqttDataConverter.convertSemiProfileA(hexString);
            case "AE_01/production/semi_profile_B":
                return MqttDataConverter.convertSemiProfileB(hexString);
            case "AE_01/signal/start":
                return MqttDataConverter.convertSignalStart(hexString) ? 1 : 0;
            case "AE_01/signal/end":
                return MqttDataConverter.convertSignalEnd(hexString) ? 1 : 0;
            case "AE_01/signal/billet_detecting":
                return MqttDataConverter.convertBilletDetecting(hexString) ? 1 : 0;
            case "AE_01/signal/heart_beat":
                return MqttDataConverter.convertHeartBeat(hexString) ? 1 : 0;
            case "AE_01/signal/puller_A":
                return MqttDataConverter.convertPullerA(hexString) ? 1 : 0;
            case "AE_01/signal/puller_B":
                return MqttDataConverter.convertPullerB(hexString) ? 1 : 0;
            case "AE_01/signal/cutter":
                return MqttDataConverter.convertCutter(hexString) ? 1 : 0;
            default:
                throw new IllegalArgumentException("Unknown topic: " + topic);
        }
    }
}