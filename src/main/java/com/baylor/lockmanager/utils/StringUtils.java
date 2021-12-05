package com.baylor.lockmanager.utils;

public class StringUtils {

	private StringUtils() {

	}

	public static String convertIntToBinary(int number) {
		String result = Integer.toBinaryString(number);
		
		return String.format("%32s", result).replaceAll(" ", "0"); // 32-bit Integer
	}

	public static String flipBit(String bit) {
		if ("0".equals(bit)) {
			return "1";
		}

		return "0";
	}

}
