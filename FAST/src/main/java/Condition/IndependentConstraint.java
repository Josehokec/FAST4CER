package Condition;

import Common.ComparedOperator;
import Common.EventSchema;

/**
 * 独立约束条件：
 * 100 <= a.open <= 165
 * b.volume > 80
 * 默认是 minValue <= value(attrName) <= maxValue
 * 假设全部都用了long类型来存
 * 注意由于RangeBitmap不支持浮点类型的存储，因此这里要做变换
 */
public class IndependentConstraint extends Constraint{

    final String attrName;
    final long minValue;
    final long maxValue;

    public IndependentConstraint(String attrName, long minValue, long maxValue){
        this.attrName = attrName;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    /**
     * varName.attrName cmp value
     * @param attrName      attrName
     * @param cmp           ComparedOperator
     * @param value         value
     * @param schema        schema
     */
    public IndependentConstraint(String attrName, ComparedOperator cmp, String value, EventSchema schema){
        this.attrName = attrName;
        int idx = schema.getAttrNameIdx(attrName);
        String attrType = schema.getIthAttrType(idx);

        long v = 0;
        long min = Long.MIN_VALUE, max = Long.MAX_VALUE;
        if(attrType.equals("INT")){
            v = Integer.parseInt(value);
        }else if(attrType.contains("FLOAT")){
            int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
            v = (int) (Float.parseFloat(value) * magnification);
        }else if(attrType.contains("DOUBLE")){
            int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
            v = (long) (Double.parseDouble(value) * magnification);
        }

        switch (cmp) {
            case LE -> max = v;
            case LT -> max = v - 1;
            case GE -> min = v;
            case GT -> min = v + 1;
        }
        this.minValue = min;
        this.maxValue = max;
    }

    /**
     * value1 <=? varName.attrName <=? value2
     * @param attrName      attribute name
     * @param cmp1          cmp1
     * @param value1        value1
     * @param cmp2          cmp2
     * @param value2        value2
     * @param schema        event schema
     */
    public IndependentConstraint(String attrName, ComparedOperator cmp1, String value1, ComparedOperator cmp2, String value2, EventSchema schema){
        this.attrName = attrName;
        int idx = schema.getAttrNameIdx(attrName);
        String attrType = schema.getIthAttrType(idx);

        long v1 = 0;
        long v2 = 0;
        long min = Long.MIN_VALUE, max = Long.MAX_VALUE;
        if(attrType.equals("INT")){
            v1 = Integer.parseInt(value1);
            v2 = Integer.parseInt(value2);
        }else if(attrType.contains("FLOAT")){
            int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
            v1 = (int) (Float.parseFloat(value1) * magnification);
            v2 = (int) (Float.parseFloat(value2) * magnification);
        }else if(attrType.contains("DOUBLE")){
            int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
            v1 = (long) (Double.parseDouble(value1) * magnification);
            v2 = (long) (Double.parseDouble(value2) * magnification);
        }

        switch (cmp1) {
            case GE -> min = v1;
            case GT -> min = v1 + 1;
            default -> System.out.println("cmp1 is illegal.");
        }
        switch(cmp2){
            case LE -> max = v2;
            case LT -> max = v2 - 1;
            default -> System.out.println("cmp2 is illegal.");
        }
        this.minValue = min;
        this.maxValue = max;
    }

    public final String getAttrName() {
        return attrName;
    }

    public final long getMinValue() {
        return minValue;
    }

    public final long getMaxValue() {
        return maxValue;
    }

    /**
     * 判断这个独立约束是否有最大最小值
     * @return 如果没有最大值和最小值则返回0；
     *         如果有最小值，没有最大值返回1；
     *         如果没有最小值，有最大值返回2；
     *         如果有最小值和最大值返回3；
     */
    public int hasMinMaxValue(){
        //ans = 0011
        int ans = 0x03;
        if(minValue == Long.MIN_VALUE){
            //把最后一位 置0
            ans &= 0xfe;
        }
        if(maxValue == Long.MAX_VALUE){
            //把倒数第二位 置0
            ans &= 0xfd;
        }
        return ans;
    }

    @Override
    public void print() {
        int val = hasMinMaxValue();
        if(val == 0){
            System.out.println("IndependentConstraint - attrName: '" + attrName + "' do not have value range constraint.");
        }else if(val == 1){
            System.out.println("IndependentConstraint - attrName: '" + attrName + "' value range is: [" + minValue+ ",INF).");
        }else if(val == 2){
            System.out.println("IndependentConstraint - attrName: '" + attrName + "' value range is: (INF, " + maxValue + "].");
        }else{
            System.out.println("IndependentConstraint - attrName: '" + attrName +
                    "' value range is: [" + minValue + "," + maxValue + "].");
        }
    }

    @Override
    public String toString(){
        int val = hasMinMaxValue();
        String ans;
        if(val == 0){
            ans = "attrName: '" + attrName + "' do not have value range constraint.";
        }else if(val == 1){
            ans = "attrName: '" + attrName + "' value range is: [" + minValue+ ",INF).";
        }else if(val == 2){
            ans = "attrName: '" + attrName + "' value range is: (INF, " + maxValue + "].";
        }else{
            ans = "attrName: '" + attrName + "' value range is: [" + minValue + "," + maxValue + "].";
        }
        return ans;
    }
}
