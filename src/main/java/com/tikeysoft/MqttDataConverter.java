package com.tikeysoft;

public class MqttDataConverter {

    public static double convertDieTemp(String hexString) {
        return hexToSignedDecimal(hexString.substring(0, 4)) * 0.1;
    }

    public static double convertBilletTemp(String hexString) {
        return hexToSignedDecimal(hexString.substring(0, 4)) * 0.1;
    }

    public static int convertRampPressure(String hexString) {
        return hexToSignedDecimal(hexString.substring(0, 4));
    }

    public static int convertBillet(String hexString) {
        return hexToSignedDecimal(hexString.substring(0, 4));
    }

    public static int convertBilletWaste(String hexString) {
        return hexToSignedDecimal(hexString.substring(0, 4));
    }

    public static double convertSemiProfileA(String hexString) {
        return hexToUnsignedDecimal(hexString.substring(4, 8)) / (40.0 * 4) * 190;
    }

    public static double convertSemiProfileB(String hexString) {
        return hexToUnsignedDecimal(hexString.substring(4, 8)) / (40.0 * 4) * 190;
    }

    public static boolean convertSignalStart(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 9);
    }

    public static boolean convertSignalEnd(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 1);
    }

    public static boolean convertBilletDetecting(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 10);
    }

    public static boolean convertHeartBeat(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 2);
    }

    public static boolean convertPullerA(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 11);
    }

    public static boolean convertPullerB(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 3);
    }

    public static boolean convertCutter(String hexString) {
        return getBitFromHex(hexString.substring(0, 4), 9);
    }

    private static int hexToSignedDecimal(String hex) {
        int value = Integer.parseInt(hex, 16);
        if ((value & 0x8000) != 0) {
            value -= 0x10000;
        }
        return value;
    }

    private static int hexToUnsignedDecimal(String hex) {
        return Integer.parseInt(hex, 16);
    }

    private static boolean getBitFromHex(String hex, int bitPosition) {
        int value = Integer.parseInt(hex, 16);
        return (value & (1 << (bitPosition - 1))) != 0;
    }
}