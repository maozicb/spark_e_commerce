package utils;

import java.math.BigDecimal;

public class NumberUtils {

    public static double formatDouble(double num, int scale) {
        BigDecimal bigDecimal = new BigDecimal(num);
        return bigDecimal.setScale(scale,BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
