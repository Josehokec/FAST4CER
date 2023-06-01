package Common;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ReservoirSampling{
    private final List<List<Long>> samples;               // 采样到的数据
    private final int maxSampleNum;                 // 最多采样多少个样本

    public ReservoirSampling(int indexNum){
        samples = new ArrayList<>();
        maxSampleNum = 5000;
        for(int i = 0; i < indexNum; ++i){
            samples.add(new ArrayList<>(maxSampleNum));
        }
    }

    /**
     * 对记录每个属性进行采样
     * long[] attrValArray = new long[indexAttrNum];
     * @param indexAttrValues 传过来的是索引属性值
     * @param recordIndices 记录索引
     */
    public final void sampling(long[] indexAttrValues, int recordIndices){
        Random random = new Random();
        if(recordIndices < maxSampleNum){
            for(int i = 0; i < indexAttrValues.length; ++i){
                samples.get(i).add(indexAttrValues[i]);
            }
        }else{
            // 从第k+1个对象开始对于第m个对象，以k/m的概率选取，并以1/k的概率替换水库中存在的一个对象
            // [0, num)
            int r = random.nextInt(recordIndices + 1);
            if (r < maxSampleNum) {
                // 替换元素
                for(int i = 0; i < indexAttrValues.length; ++i){
                    samples.get(i).set(r, indexAttrValues[i]);
                }
            }
        }
    }

    /**
     * 估算选择率
     * @param indexId 索引的id
     * @param min 最小值
     * @param max 最大值
     * @return 选择率
     */
    public final double selectivity(int indexId, long min, long max){
        int cnt = 0;
        List<Long> sampleAttrList = samples.get(indexId);
        int sampleNum = sampleAttrList.size();
        for(long value : sampleAttrList){
            if(value >= min && value <= max){
                cnt++;
            }
        }
        return (cnt + 0.0) / sampleNum;
    }
}
