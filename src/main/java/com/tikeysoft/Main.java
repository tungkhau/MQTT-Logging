package com.tikeysoft;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class Main {
    public static void main(String[] args) {
        String broker = "tcp://localhost:1883";
        String clientId = "Mqtt_logging";

        String[] topics = {"AE_01/condition/die_temp",
                "AE_01/condition/billet_temp",
                "AE_01/condition/ramp_pressure",
                "AE_01/production/billet",
                "AE_01/production/billet_detecting",
                "AE_01/production/billet_waste",
                "AE_01/production/semi_profile_head",
                "AE_01/production/semi_profile_tail",
                "AE_01/production/semi_profile_start",
                "AE_01/production/semi_profile_end",
                "AE_01/operation/signal"
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
                    System.out.println("Message arrived. Topic: " + topic + " Message: " + new String(message.getPayload()));
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
}