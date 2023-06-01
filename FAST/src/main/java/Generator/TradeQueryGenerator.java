package Generator;

import net.sf.json.JSONArray;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TradeQueryGenerator {

    public void generateQuery(String filePath, int queryNUm){
        /*
        volume range:
        YHOO (Id : 1): 1 7600             RIMM (Id : 2): 1 ~ 4800, 7200      QQQ (Id : 3): 1  ~ 100000           ORCL (Id : 4): 1 ~ 30000, 34560
        MSFT (Id : 5): 1 ~ 27100, 38232   IPIX (Id : 6): 2 ~ 20000           INTC (Id : 7): 1 ~ 34000, 10000     DELL (Id : 8): 1 ~ 9000, 14400
        CSCO (Id : 9): 1 ~ 19686, 23850   AMAT (Id : 10): 1 ~ 22560

        price
        YHOO (Id : 1): 31.06 31.81        RIMM (Id : 2): 111.86, 111.87 ~ 117.00, 117.47
        QQQ (Id : 3): 35.844 ~ 36.361     ORCL (Id : 4): 10.85 ~ 11.21
        MSFT (Id : 5): 25.84 ~ 26.12      IPIX (Id : 6): 7.55 ~ 13.04
        INTC (Id : 7): 27.34 ~ 28.35      DELL (Id : 8): 35.10 ~ 35.65
        CSCO (Id : 9): 22.21 ~ 22.62      AMAT (Id :10): 18.58 ~ 18.99
        */
        // 0.5% 0.25% 0.1% 0.05%
        double[] scales1 = {1.005, 1.0025, 1.001, 1.0005};
        double[] scales2 = {0.995, 0.9975, 0.999, 0.9995};
        // 10 seconds, 8 seconds, 6 seconds, 4 seconds
        long[] windows = {10000, 8000, 6000, 4000};

        int cnt = queryNUm / 2;
        Random random = new Random();
        List<String> queries = new ArrayList<>(queryNUm);

        // B v0, B v1, B v2, B v3
        for(int i = 0; i < cnt; ++i) {
            StringBuilder query = new StringBuilder(256);
            query.append("PATTERN SEQ(B v0, B v1, B v2, B v3)\n");
            query.append("FROM trade\n");
            query.append("USING SKIP_TILL_ANY_MATCH\n");
            query.append("WHERE ");

            int id1 = random.nextInt(1, 11);
            int id2 = random.nextInt(1, 11);
            while(id2 == id1){
                id2 = random.nextInt(1, 11);
            }

            // x_min <= v0.price <= x_max AND  100 <= v0.volume <= 100000
            query.append(id1).append(" <= v").append(0).append(".stockID").append(" <= ").append(id1);
            query.append(" AND ");
            query.append(id2).append(" <= v").append(1).append(".stockID").append(" <= ").append(id2);
            query.append(" AND ");
            query.append(id1).append(" <= v").append(2).append(".stockID").append(" <= ").append(id1);
            query.append(" AND ");
            query.append(id2).append(" <= v").append(3).append(".stockID").append(" <= ").append(id2);
            query.append(" AND ");

            int scaleIdx = random.nextInt(0, 4);
            query.append("v3.price >= v1.price * ").append(scales1[scaleIdx]);
            query.append(" AND ");
            query.append("v2.price <= v0.price * ").append(scales2[scaleIdx]).append("\n");

            int windowIdx = random.nextInt(0, 4);
            query.append("WITHIN ").append(windows[windowIdx]).append(" units\n");
            query.append("RETURN COUNT(*)");
            System.out.println(query);
            queries.add(query.toString());
        }

        // S v0, S v1, S v2, S v3
        for(int i = 0; i < cnt; ++i) {
            StringBuilder query = new StringBuilder(256);
            query.append("PATTERN SEQ(S v0, S v1, S v2, S v3)\n");
            query.append("FROM trade\n");
            query.append("USING SKIP_TILL_ANY_MATCH\n");
            query.append("WHERE ");

            int id1 = random.nextInt(1, 11);
            int id2 = random.nextInt(1, 11);
            while(id2 == id1){
                id2 = random.nextInt(1, 11);
            }

            query.append(id1).append(" <= v").append(0).append(".stockID").append(" <= ").append(id1);
            query.append(" AND ");
            query.append(id2).append(" <= v").append(1).append(".stockID").append(" <= ").append(id2);
            query.append(" AND ");
            query.append(id1).append(" <= v").append(2).append(".stockID").append(" <= ").append(id1);
            query.append(" AND ");
            query.append(id2).append(" <= v").append(3).append(".stockID").append(" <= ").append(id2);
            query.append(" AND ");

            int scaleIdx = random.nextInt(0, 4);
            query.append("v3.price >= v1.price * ").append(scales1[scaleIdx]);
            query.append(" AND ");
            query.append("v2.price <= v0.price * ").append(scales2[scaleIdx]).append("\n");

            int windowIdx = random.nextInt(0, 4);
            query.append("WITHIN ").append(windows[windowIdx]).append(" units\n");
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
        TradeQueryGenerator tqg = new TradeQueryGenerator();
        // random generate 300 queries
        tqg.generateQuery("trade_query.json", 300);
    }

}
