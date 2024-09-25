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

        // validate 11-16 are floats, they sum to total, and total <= 500
        try {
            float cur_sum = 0;
            float total = Float.parseFloat(fields[16].trim());
            for (int i = 11; i < 16; i++) {
                cur_sum += Float.parseFloat(fields[i].trim());
            }

            if (Math.abs(cur_sum - total) > 0.001 || total > 500) {
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
     * errors[0] will be true if pick-up gps position is error
     * errors[1] will be true if drop-off gps position is error
     */
    public static boolean[] countGPSErrors(String[] fields) {
        boolean[] errors = new boolean[2];

        // check for errors in pick-up (fields[6] and fields[7]) and drop-off (fields[8] and fields[9])
        for (int i = 6; i <= 7; i++) {
            if (fields[i].length() == 0 || Float.parseFloat(fields[i]) == 0) {
                errors[0] = true;
                break;  
            }
        }

        for (int i = 8; i <= 9; i++) {
            if (fields[i].length() == 0 || Float.parseFloat(fields[i]) == 0) {
                errors[1] = true;
                break; 
            }
        }

        return errors;
    }
}
