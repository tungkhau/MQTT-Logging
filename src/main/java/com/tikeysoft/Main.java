package com.tikeysoft;

import org.eclipse.paho.client.mqttv3.MqttException;

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
                "AE_01/signal/end",
                "AE_01/signal/billet_detecting",
                "AE_01/signal/heart_beat",
                "AE_01/signal/puller_A",
                "AE_01/signal/puller_B",
                "AE_01/signal/cutter"
        };

        MqttHandler mqttHandler = new MqttHandler();
        try {
            mqttHandler.connectAndSubscribe(broker, clientId, topics);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}