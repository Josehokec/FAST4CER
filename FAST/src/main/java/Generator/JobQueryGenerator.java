package Generator;

import net.sf.json.JSONArray;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class JobQueryGenerator {

    public void generateQuery(String filePath, int queryNUm){
        /*
        volume
        YHOO (Id : 1): 1 7600
        RIMM (Id : 2): 1 ~ 4800, 7200
        QQQ (Id : 3): 1  ~ 100000
        ORCL (Id : 4): 1 ~ 30000, 34560
        MSFT (Id : 5): 1 ~ 27100, 38232
        IPIX (Id : 6): 2 ~ 20000
        INTC (Id : 7): 1 ~ 34000, 10000
        DELL (Id : 8): 1 ~ 9000, 14400
        CSCO (Id : 9): 1 ~ 19686, 23850
        AMAT (Id : 10): 1 ~ 22560

        price
        YHOO (Id : 1): 31.06 31.81
        RIMM (Id : 2): 111.86, 111.87 ~ 117.00, 117.47
        QQQ (Id : 3): 35.844 ~ 36.361
        ORCL (Id : 4): 10.85 ~ 11.21
        MSFT (Id : 5): 25.84 ~ 26.12
        IPIX (Id : 6): 7.55 ~ 13.04
        INTC (Id : 7): 27.34 ~ 28.35
        DELL (Id : 8): 35.10 ~ 35.65
        CSCO (Id : 9): 22.21 ~ 22.62
        AMAT (Id :10): 18.58 ~ 18.99
        */

        // 30s -> 30_000, 1min -> 60_000, 2min -> 120_000, 5min -> 300_000
        long[] windows = {30_000L, 60_000L, 120_000L, 300_000L};

        final int cnt = queryNUm / 3;
        Random random = new Random();
        List<String> queries = new ArrayList<>(queryNUm);

        // SUBMIT -> SCHEDULE -> FINISH
        for(int i = 0; i < cnt; ++i) {
            StringBuilder query = new StringBuilder(256);
            // SUBMIT 0, SCHEDULE 1, FINISH 4
            query.append("PATTERN SEQ(0 v0, 1 v1, 4 v2)\n");
            query.append("FROM job\n");
            query.append("USING SKIP_TILL_NEXT_MATCH\n");
            query.append("WHERE ");

            // schedulingClass 0 ~ 3
            int schedulingClass = random.nextInt(0, 4);

            // x_min <= v0.price <= x_max AND  100 <= v0.volume <= 100000
            query.append(schedulingClass).append(" <= v").append(0).append(".schedulingClass").append(" <= ").append(schedulingClass);
            query.append(" AND ");
            query.append(schedulingClass).append(" <= v").append(1).append(".schedulingClass").append(" <= ").append(schedulingClass);
            query.append(" AND ");
            query.append(schedulingClass).append(" <= v").append(1).append(".schedulingClass").append(" <= ").append(schedulingClass);
            query.append(" AND ");
            query.append("v0.jobID = v1.jobID");
            query.append(" AND ");
            query.append("v1.jobID = v2.jobID\n");

            int w = random.nextInt(0, 4);

            query.append("WITHIN ").append(windows[w]).append(" units\n");
            query.append("RETURN COUNT(*)");
            System.out.println(query);
            queries.add(query.toString());
        }

        // SUBMIT -> SCHEDULE -> KILL
        for(int i = 0; i < cnt; ++i) {
            StringBuilder query = new StringBuilder(256);
            // SUBMIT 0, SCHEDULE 1, KILL 4
            query.append("PATTERN SEQ(0 v0, 1 v1, 5 v2)\n");
            query.append("FROM job\n");
            query.append("USING SKIP_TILL_NEXT_MATCH\n");
            query.append("WHERE ");

            // schedulingClass 0 ~ 3
            int schedulingClass = random.nextInt(0, 4);

            // x_min <= v0.price <= x_max AND  100 <= v0.volume <= 100000
            query.append(schedulingClass).append(" <= v").append(0).append(".schedulingClass").append(" <= ").append(schedulingClass);
            query.append(" AND ");
            query.append(schedulingClass).append(" <= v").append(1).append(".schedulingClass").append(" <= ").append(schedulingClass);
            query.append(" AND ");
            query.append(schedulingClass).append(" <= v").append(1).append(".schedulingClass").append(" <= ").append(schedulingClass);
            query.append(" AND ");
            query.append("v0.jobID = v1.jobID");
            query.append(" AND ");
            query.append("v1.jobID = v2.jobID\n");

            int w = random.nextInt(0, 4);

            query.append("WITHIN ").append(windows[w]).append(" units\n");
            query.append("RETURN COUNT(*)");
            System.out.println(query);
            queries.add(query.toString());
        }

        // SUBMIT -> SCHEDULE -> EVICT
        for(int i = 0; i < cnt; ++i) {
            StringBuilder query = new StringBuilder(256);
            // SUBMIT 0, SCHEDULE 1, KILL 4
            query.append("PATTERN SEQ(0 v0, 1 v1, 2 v2)\n");
            query.append("FROM job\n");
            query.append("USING SKIP_TILL_NEXT_MATCH\n");
            query.append("WHERE ");

            // schedulingClass 0 ~ 3
            int schedulingClass = random.nextInt(0, 4);

            // x_min <= v0.price <= x_max AND  100 <= v0.volume <= 100000
            query.append(schedulingClass).append(" <= v").append(0).append(".schedulingClass").append(" <= ").append(schedulingClass);
            query.append(" AND ");
            query.append(schedulingClass).append(" <= v").append(1).append(".schedulingClass").append(" <= ").append(schedulingClass);
            query.append(" AND ");
            query.append(schedulingClass).append(" <= v").append(1).append(".schedulingClass").append(" <= ").append(schedulingClass);
            query.append(" AND ");
            query.append("v0.jobID = v1.jobID");
            query.append(" AND ");
            query.append("v1.jobID = v2.jobID\n");

            int w = random.nextInt(0, 4);

            query.append("WITHIN ").append(windows[w]).append(" units\n");
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
        JobQueryGenerator jqg = new JobQueryGenerator();
        // random generate 300 queries
        jqg.generateQuery("job_query.json", 300);
    }
}
