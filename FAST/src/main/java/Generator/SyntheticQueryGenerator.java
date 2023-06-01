package Generator;

import net.sf.json.JSONArray;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * generate random query for synthetic dataset
 * [mu - 3 * sigma, mu - 2 * sigma] 2.15%
 * [mu - 2 * sigma, mu - sigma] 13.6%
 * [mu - sigma, mu] 34.1%
 * attrDataTypes = {"INT_UNIFORM", "INT_GAUSSIAN", "DOUBLE_UNIFORM", "DOUBLE_GAUSSIAN"};
 * range [0,1000]
 * generate query patterns for synthetic dataset
 * patternLen \in [2,8],
 * icNum \in [1,4],
 * sel \in \{0.01, 0.0215, 0.05, 0.136\},
 * \tau \in \{1000, 2000, 3000, 4000, 5000\}
 */
public class SyntheticQueryGenerator {
    public static boolean debug = true;
    private final int eventTypeNum;
    private final Random random;
    public SyntheticQueryGenerator(int eventTypeNum) {
        this.eventTypeNum = eventTypeNum;
        random = new Random();
    }

    public void generateQuery(String filePath, int queryNUm){
        List<String> queries = new ArrayList<>(queryNUm);
        // data set alpha is 0.6


        long[] windows = {1000, 2000, 3000, 4000};

        for(int i = 0; i < queryNUm; ++i){
            StringBuilder query = new StringBuilder(256);
            // patternLen: 3 ~ 8
            final int patternLen = random.nextInt(3,9);
            query.append("PATTERN SEQ(");
            for(int xi = 0; xi < patternLen; ++xi){
                int id = random.nextInt(0, 30);
                query.append("TYPE_").append(id).append(" v").append(xi);

                if(xi != patternLen - 1){
                    query.append(", ");
                }else{
                    query.append(")\n");
                }
            }

            query.append("FROM synthetic\n");

            int s = random.nextInt(2, 4);
            if(s == 3){
                query.append("USING SKIP_TILL_ANY_MATCH\n");
            }else{
                query.append("USING SKIP_TILL_NEXT_MATCH\n");
            }

            query.append("WHERE ");
            // ICList
            for(int xi = 0; xi < patternLen; ++xi){
                // attribute selectivity: {0.05, 0.136, 0.2, 0.341}
               for(int k = 1; k <= 4; ++k){
                    double choose = random.nextDouble();
                    if(choose <= 0.8 - 0.15 * k){
                        switch (k) {
                            case 1 -> query.append("0 <= v").append(xi).append(".a1 <= 50 AND ");
                            case 2 -> query.append("200 <= v").append(xi).append(".a2 <= 350 AND ");
                            case 3 -> query.append("0 <= v").append(xi).append(".a3 <= 200 AND ");
                            case 4 -> query.append("350 <= v").append(xi).append(".a4 <= 500 AND ");
                        }
                    }
               }
            }

            // random choose 1 ~ 3 DC
            final int dcNum = random.nextInt(1, 4);
            for(int j = 0; j < dcNum; ++j){
                int var1 = random.nextInt(patternLen);
                int var2 = random.nextInt(patternLen);
                if(var2 == var1){
                    var2 = (var2 + 1) % patternLen;
                }
                int attrId = random.nextInt(1, 5);
                query.append("v").append(var1).append(".a").append(attrId).append(" <= ").append("v").append(var2).append(".a").append(attrId);
                if(j != dcNum - 1){
                    query.append(" AND ");
                }else{
                    query.append("\n");
                }
            }

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
        SyntheticQueryGenerator qg = new SyntheticQueryGenerator(50);
        // random generate 10000 queries
        qg.generateQuery("synthetic_query.json", 301);
    }
}

/*
"PATTERN SEQ(IBM a, IBM b, IBM c)
FROM stock
USING SKIP_TILL_NEXT_MATCH
WHERE 50.1 <= a.open <= 70.1 AND 60 <= b.open <= 80 AND b.volume >= 100 AND c.open >= 40 AND 80 <= c.volume <= 120
WITHIN 10 mins
RETURN COUNT(*)"
 */
