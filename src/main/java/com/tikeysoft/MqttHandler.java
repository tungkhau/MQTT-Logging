package com.tikeysoft;

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
    private double billetCache = 0.0;
    private double billetWasteCache = 0.0;
    private double semiProfileACache = 0.0;
    private double semiProfileBCache = 0.0;

    private boolean heartBeat = false;
    private boolean cutter = false;

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
        switch (topic) {
            // Signal
            case "AE_01/signal/heart_beat":
                heartBeat = MqttDataConverter.convertHeartBeat(hexString);
                break;
            case "AE_01/signal/cutter":
                cutter = MqttDataConverter.convertCutter(hexString);
                break;

            // Publish condition values
            case "AE_01/condition/die_temp":
                if (!heartBeat) return;
                client.publish("processed/AE_01/condition/die_temp", new MqttMessage(String.valueOf(MqttDataConverter.convertDieTemp(hexString)).getBytes()));
                break;
            case "AE_01/condition/billet_temp":
                if (!heartBeat) return;
                client.publish("processed/AE_01/condition/billet_temp", new MqttMessage(String.valueOf(MqttDataConverter.convertBilletTemp(hexString)).getBytes()));
                break;
            case "AE_01/condition/ramp_pressure":
                if (!heartBeat) return;
                client.publish("processed/AE_01/condition/ramp_pressure", new MqttMessage(String.valueOf(MqttDataConverter.convertRampPressure(hexString)).getBytes()));
                break;

            // Cache values
            case "AE_01/production/billet":
                billetCache = MqttDataConverter.convertBillet(hexString);
                break;
            case "AE_01/production/billet_waste":
                billetWasteCache = MqttDataConverter.convertBilletWaste(hexString);
                break;
            case "AE_01/production/semi_profile_A":
                semiProfileACache = MqttDataConverter.convertSemiProfileA(hexString);
                break;
            case "AE_01/production/semi_profile_B":
                semiProfileBCache = MqttDataConverter.convertSemiProfileB(hexString);
                break;

            // Process Billet value
            case "AE_01/signal/billet_detecting":
                if (!MqttDataConverter.convertBilletDetecting(hexString)) return;
                client.publish("processed/AE_01/production/billet", new MqttMessage(String.valueOf(billetCache).getBytes()));
                break;

            // Process Billet waste value
            case "AE_01/signal/end":
                if (!MqttDataConverter.convertSignalEnd(hexString)) return;
                client.publish("processed/AE_01/production/billet_waste", new MqttMessage(String.valueOf(billetWasteCache).getBytes()));
                break;

            // Process Semi profile value
            case "AE_01/signal/puller_A":
                if (heartBeat && MqttDataConverter.convertPullerA(hexString) && !cutter) {
                    client.publish("processed/AE_01/production/semi_profile", new MqttMessage(String.valueOf(semiProfileBCache).getBytes()));
                }
                break;
            case "AE_01/signal/puller_B":
                if (heartBeat && MqttDataConverter.convertPullerB(hexString) && !cutter) {
                    client.publish("processed/AE_01/production/semi_profile", new MqttMessage(String.valueOf(semiProfileACache).getBytes()));
                }
                break;

            default:
                throw new IllegalArgumentException("Unknown topic: " + topic);
        }
    }
}