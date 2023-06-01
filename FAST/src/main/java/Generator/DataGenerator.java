package Generator;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

/**
 * int -> Uniform, int -> Gaussian, double -> uniform, double -> Gaussian
 */
public class DataGenerator {
    public static boolean debug = true;
    private final String[] attrDataTypes;
    private final int eventTypeNum;
    private final Random random;

    public DataGenerator(String[] attrDataTypes, int eventTypeNum) {
        this.attrDataTypes = attrDataTypes;
        this.eventTypeNum = eventTypeNum;
        random = new Random();
    }

    public void generateDataset(String filePath, int recordNum){
        long startTime = System.currentTimeMillis();
        double[] probability = zipfProbability(1.6);

        int minVal = 0;
        int maxVal = 1000;
        int mu = 500;
        int sigma = 150;

        File file = new File(filePath);

        try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, false))){
            // output format: eventType,attribute^1,...,attribute^d,timestamp
            for(int i = 0; i < recordNum; ++i){
                StringBuilder record = new StringBuilder(128);

                boolean noAppendType = true;
                double pro = random.nextDouble();
                for(int j = 0; j < eventTypeNum; ++j){
                    if(pro < probability[j]){
                        record.append("TYPE_").append(j).append(",");
                        noAppendType = false;
                        break;
                    }else{
                        pro -= probability[j];
                    }
                }
                if(noAppendType){
                    record.append("TYPE_").append(eventTypeNum - 1).append(",");
                }

                for (String attrDataType : attrDataTypes) {
                    switch (attrDataType) {
                        case "INT_UNIFORM" -> record.append(getUniformInteger(minVal, maxVal)).append(",");
                        case "INT_GAUSSIAN" -> record.append(getGaussianInteger(minVal, maxVal, mu, sigma)).append(",");
                        case "DOUBLE_UNIFORM" -> {
                            String value = String.format("%.2f", getUniformDouble(minVal, maxVal));
                            record.append(value).append(",");
                        }
                        case "DOUBLE_GAUSSIAN" -> {
                            String value = String.format("%.2f", getGaussianDouble(minVal, maxVal, mu, sigma));
                            record.append(value).append(",");
                        }
                        default -> throw new RuntimeException("do not support this data type");
                    }
                }

                long curTime = startTime + i;
                record.append(curTime).append("\n");

                out.write(record.toString().getBytes());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("generate data finished.");
    }

    /**
     * generate zipf distribution
     * @param   alpha   skew param
     * @return          probability
     */
    public double[] zipfProbability(double alpha){
        double[] ans = new double[eventTypeNum];
        double C = 0;
        for(int i = 1; i <= eventTypeNum; ++i){
            C += Math.pow((1.0 / i), alpha);
        }
        double sumPro = 0;
        for(int i = 1; i <= eventTypeNum; ++i){
            double pro = 1.0 / (Math.pow(i, alpha) * C);
            ans[i - 1] = pro;
            sumPro += pro;
        }
        if(debug){
            System.out.println("zipf skew: " + alpha);
            System.out.print("zipf probability:\n[");
            for(int i = 0; i < eventTypeNum - 1; ++i){
                String value = String.format("%.4f", ans[i]);
                System.out.print(value + ",");
            }
            String value = String.format("%.4f", ans[eventTypeNum - 1]);
            System.out.println(value + "]");
            System.out.println("zipf sum probability: " + sumPro);
        }
        return ans;
    }

    public final int getUniformInteger(int minVal, int maxVal){
        // [minVal, maxVal + 1) -> [minVal, maxVal]
        return random.nextInt(minVal, maxVal + 1);
    }

    public final int getGaussianInteger(int minVal, int maxVal, double mu, double sigma){
        // x ~(mu, sigma) & minVal <= x <= maxVal
        int value = (int) random.nextGaussian(mu, sigma);
        if(value < minVal){
            value = minVal;
        }else if(value > maxVal){
            value = maxVal;
        }
        return value;
    }

    public final double getUniformDouble(double minVal, double maxVal){
        return random.nextDouble(minVal, maxVal);
    }

    public final double getGaussianDouble(int minVal, int maxVal, double mu, double sigma){
        // x ~(mu, sigma) & minVal <= x <= maxVal
        double value =  random.nextGaussian(mu, sigma);
        if(value < minVal){
            value = minVal;
        }else if(value > maxVal){
            value = maxVal;
        }
        return value;
    }

    public static void main(String[] args){
        String[] attrDataTypes = {"INT_UNIFORM", "INT_GAUSSIAN", "DOUBLE_UNIFORM", "DOUBLE_GAUSSIAN"};
        int eventTypeNum = 50;

        String dir = System.getProperty("user.dir");
        String filePath = dir + File.separator + "src" + File.separator + "main" +
                File.separator + "dataset" + File.separator + "synthetic_6M.csv";

        DataGenerator generator = new DataGenerator(attrDataTypes, eventTypeNum);
        // synthetic_1M.csv
        // 2_000_000 (2M), 4_000_000 (4M),
        //8_000_000 (8M), 16_000_000 (16M), 32_000_000 (32M)
        generator.generateDataset(filePath, 6_000_000);
    }
}

