package edu.cs.utexas.HadoopEx;

public class Utils {
    /*
     * filtering logic
     */
    public static boolean validLine(String[] fields) {
        // length
        if(fields.length != 17) {
            return false;
        }

        // trip time in secs is 0
        try {
            if (Float.parseFloat(fields[4]) == 0) {
                return false;
            }
        } catch (Exception e) {
            // not a float, invalid input
            return false;
        }

        // validate 11-16 are floats and sums to total
        try {
            float cur_sum = 0;
            float total = Float.parseFloat(fields[16].trim());
            for (int i = 11; i < 16; i++) {
                cur_sum += Float.parseFloat(fields[i].trim());
            }

            if (Math.abs(cur_sum - total) > 0.001) {
                return false;
            }
        } catch (Exception e) {
            // not a float, invalid input
            return false;
        }

        return true;
    }

    /*
     * check if gps position is error
     */
    public static boolean isGPSError(String[] fields) {
        for (int i = 6; i < 10; i++) {
            // empty
            if (fields[i].length() == 0) {
                return true;
            }

            // zero
            if (Float.parseFloat(fields[i]) == 0) {
                return true;
            }
        }
        return false;
    }
}
