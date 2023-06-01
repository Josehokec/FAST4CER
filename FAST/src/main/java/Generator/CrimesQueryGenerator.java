package Generator;

import net.sf.json.JSONArray;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CrimesQueryGenerator {
    public void generateQuery(String filePath, int queryNUm){
        final int cnt = queryNUm / 3;
        Random random = new Random();
        List<String> queries = new ArrayList<>(queryNUm);
        long windows = 108000; // 30 Minutes
        // within a specific Beat
        for(int i = 0; i < cnt; ++i) {
            StringBuilder query = new StringBuilder(256);
            final int patternLen = 3;

            query.append("PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)\n");
            query.append("FROM crimes\n");
            query.append("USING SKIP_TILL_NEXT_MATCH\n");
            query.append("WHERE ");
            int start = random.nextInt(200, 2500);
            for (int xi = 0; xi < patternLen; ++xi) {
                // 111 <= Beat <= 2535  -> start , start + 10
                query.append(start).append(" <= v").append(xi).append(".Beat").append(" <= ").append(start + 30);
                if (xi != patternLen - 1) {
                    query.append(" AND ");
                } else {
                    query.append("\n");
                }
            }

            query.append("WITHIN ").append(windows).append(" units\n");
            query.append("RETURN COUNT(*)");
            System.out.println(query);
            queries.add(query.toString());
        }

        // within a specific District
        for(int i = 0; i < cnt; ++i) {
            StringBuilder query = new StringBuilder(256);
            // patternLen: 3 ~ 8
            final int patternLen = 3;

            query.append("PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)\n");
            query.append("FROM crimes\n");
            query.append("USING SKIP_TILL_ANY_MATCH\n");

            query.append("WHERE ");
            int start = random.nextInt(1, 30);
            for (int xi = 0; xi < patternLen; ++xi) {
                // 1<= District<=31  -> [start , start + 1]
                query.append(start).append(" <= v").append(xi).append(".District").append(" <= ").append(start + 1);
                if (xi != patternLen - 1) {
                    query.append(" AND ");
                } else {
                    query.append("\n");
                }
            }

            query.append("WITHIN ").append(windows).append(" units\n");
            query.append("RETURN COUNT(*)");
            System.out.println(query);
            queries.add(query.toString());
        }

        // within a specific Latitude and Longitude
        for(int i = 0; i < cnt; ++i) {
            StringBuilder query = new StringBuilder(256);
            final int patternLen = 3;
            query.append("PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)\n");
            query.append("FROM crimes\n");
            query.append("USING SKIP_TILL_NEXT_MATCH\n");

            query.append("WHERE ");
            // 41.811190492 <= v0.Latitude <= 41.861190492 AND -87.630445713 <= v0.Longitude <= -87.580445713
            double startLatitude = random.nextDouble(41.80, 41.84);
            String minLatitude = String.format("%.9f", startLatitude);
            String maxLatitude = String.format("%.9f", startLatitude + 0.04);

            double startLongitude = random.nextDouble(-87.63, -87.59);
            String minLongitude = String.format("%.9f", startLongitude);
            String maxLongitude = String.format("%.9f", startLongitude + 0.04);

            // String.format("%.4f", mergeCost);
            for (int xi = 0; xi < patternLen; ++xi) {
                query.append(minLatitude).append(" <= v").append(xi).append(".Latitude").append(" <= ").append(maxLatitude);
                query.append(" AND ");
                query.append(minLongitude).append(" <= v").append(xi).append(".Longitude").append(" <= ").append(maxLongitude);
                if (xi != patternLen - 1) {
                    query.append(" AND ");
                } else {
                    query.append("\n");
                }

            }

            query.append("WITHIN ").append(windows).append(" units\n");
            query.append("RETURN COUNT(*)");
            System.out.println(query);
            queries.add(query.toString());
        }

        JSONArray jsonArray = JSONArray.fromObject(queries);
        // JSONObject jsonArray = JSONObject.fromObject(queries);
        FileWriter fileWriter = null;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonArray.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                fileWriter.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args){
        CrimesQueryGenerator cqg = new CrimesQueryGenerator();
        // random generate 300 queries
        cqg.generateQuery("crimes_query.json", 300);
    }
}
