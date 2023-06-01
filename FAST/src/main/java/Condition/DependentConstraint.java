package Condition;

import Common.ArithmeticOperator;
import Common.ComparedOperator;
import Common.EventSchema;

/**
 * 一共12个变量，统一格式如下：
 * varName1.attrName ao1 m1 ao2 a1 LE varName2.attrName ao3 m2 ao4 a2
 * 由于RangeBitmap原因，a1 & a2 会统一成long格式
 * 目前支持的依赖谓词形式如下:
 * a.open * 3 + 5 <= b.open * 4 - 5
 * a.open + 3 <= b.open
 * a.open <= b.open
 * a.open + 4 >= b.open * 5.0
 * 不支持格式: a.open <= b.open + c.open | a.open <= b.open <= c.open | 2 * a.open + 4 <= 4 * b.open + 9
 * a.open 一定要写在前面 不然程序会炸
 */
public class DependentConstraint extends Constraint{
    private String attrName;            // 属性名字
    private ComparedOperator cmp;       // 比较符号
    private String varName1;            // 变量名字1
    private String varName2;            // 变量名字2
    private double m1;              // 倍数
    private long a1;
    private double m2;              // 倍数2
    private long a2;
    ArithmeticOperator ao1, ao2, ao3, ao4;

    public DependentConstraint(){
        attrName = null;
        varName1 = null;
        varName2 = null;
        cmp = null;
        m1 = m2 = 1;
        a1 = a2 = 0;
        ao1 = ao3 = ArithmeticOperator.MUL;
        ao2 = ao4 = ArithmeticOperator.ADD;
    }

    /**
     * 形式为a.open < b.open的构造函数<br>
     * 如果是a.open + 3 < b.open + 4这种形式的话 推荐传入字符串来构造
     * @param attrName 属性名字
     * @param varName1 变量名字1
     * @param varName2 变量名字2
     * @param cmp 比较算子
     */
    public DependentConstraint(String attrName, String varName1, String varName2, ComparedOperator cmp){
        this.attrName = attrName;
        this.varName1 = varName1;
        this.varName2 = varName2;
        this.cmp = cmp;
        m1 = m2 = 1;
        a1 = a2 = 0;
        ao1 = ao3 = ArithmeticOperator.MUL;
        ao2 = ao4 = ArithmeticOperator.ADD;
    }

    public void serCmp(ComparedOperator cmp){
        this.cmp = cmp;
    }

    public String getAttrName(){
        return attrName;
    }

    public String getVarName1(){
        return varName1;
    }

    public String getVarName2(){
        return varName2;
    }

    /**
     * 构造左边的表达式 <br>
     * 即把attrName varName1 ao1 m1 ao2 a1赋值<br>
     * 支持的格式有：a.open * 3 + 3 ｜ a.open + 4 ｜ a.open * 3 <br>
     * 不支持 3 * a.open + 4 和 4 + a.open
     * 也就是说varName.attrName必须写在前面
     * @param left 四则运算
     * @param s 事件模式
     */
    public void constructLeft(String left, EventSchema s){
        //点的位置
        int dotPos = -1;
        boolean readAttrName = false;
        int hasMulOrDiv = -1;
        int hasAddOrSub = -1;
        for(int i = 0; i < left.length(); ++i){
            if(left.charAt(i) == '.' && dotPos == -1){
                dotPos = i;
                varName1 = left.substring(0, dotPos).trim();
            }else if(left.charAt(i) == '*' || left.charAt(i) == '/'){
                hasMulOrDiv = i;
                if(!readAttrName){
                    attrName = left.substring(dotPos + 1, hasMulOrDiv).trim();
                    readAttrName = true;
                }
                ao1 = left.charAt(i) == '*' ? ArithmeticOperator.MUL : ArithmeticOperator.DIV;
            }else if(left.charAt(i) == '+' || left.charAt(i) == '-'){
                hasAddOrSub = i;
                if(!readAttrName){
                    attrName = left.substring(dotPos + 1, hasAddOrSub).trim();
                }
                ao2 = left.charAt(i) == '+' ? ArithmeticOperator.ADD : ArithmeticOperator.SUB;
                String a1Str = left.substring(i + 1).trim();

                int idx = s.getAttrNameIdx(attrName);
                if(s.getIthAttrType(idx).equals("INT")){
                    a1 = Long.parseLong(a1Str);
                }else if(s.getIthAttrType(idx).contains("FLOAT")){
                    int magnification = (int) Math.pow(10, s.getIthDecimalLens(idx));
                    a1 = (long) (Float.parseFloat(a1Str) * magnification);
                }else if(s.getIthAttrType(idx).contains("Double")){
                    int magnification = (int) Math.pow(10, s.getIthDecimalLens(idx));
                    a1 = (long) (Double.parseDouble(a1Str) * magnification);
                }

                //a1 = (long) (Double.parseDouble(a1Str));
                break;
            }
        }

        if(attrName == null){
            attrName = left.substring(dotPos + 1).trim();
        }

        //注意之前没有把m1赋值 默认是1
        if(hasMulOrDiv != -1){
            String m1Str;
            if(hasAddOrSub == -1){
                m1Str = left.substring(hasMulOrDiv + 1).trim();
            }else{
                m1Str = left.substring(hasMulOrDiv + 1, hasAddOrSub);
            }
            m1 = Double.parseDouble(m1Str);
        }
    }

    /**
     * 构造右边的表达式 <br>
     * 即把attrName varName2 ao3 m2 ao4 a2赋值<br>
     * 支持的格式有：b.open * 3 + 3 ｜ b.open + 4 ｜ b.open * 3 <br>
     * 不支持 3 * b.open + 4 和 4 + b.open<br>
     * 也就是说varName.attrName必须写在前面
     * @param right 四则运算
     * @param s 事件模式
     */
    public void constructRight(String right, EventSchema s){
        //点的位置
        int dotPos = -1;
        boolean readAttrName = false;
        int hasMulOrDiv = -1;
        int hasAddOrSub = -1;
        String rightAttrName;
        for(int i = 0; i < right.length(); ++i){
            if(right.charAt(i) == '.' && dotPos == -1){
                dotPos = i;
                varName2 = right.substring(0, dotPos).trim();
            }else if(right.charAt(i) == '*' || right.charAt(i) == '/'){
                hasMulOrDiv = i;
                if(!readAttrName){
                    readAttrName = true;
                    rightAttrName = right.substring(dotPos + 1, hasMulOrDiv).trim();


                    if(!attrName.equals(rightAttrName)){
                        throw new RuntimeException("Illegal dependent constraint");
                    }
                }
                ao3 = right.charAt(i) == '*' ? ArithmeticOperator.MUL : ArithmeticOperator.DIV;
            }else if(right.charAt(i) == '+' || right.charAt(i) == '-'){
                hasAddOrSub = i;
                if(!readAttrName){
                    rightAttrName = right.substring(dotPos + 1, hasAddOrSub).trim();
                    if(!attrName.equals(rightAttrName)){
                        throw new RuntimeException("Illegal dependent constraint");
                    }
                }
                ao4 = right.charAt(i) == '+' ? ArithmeticOperator.ADD : ArithmeticOperator.SUB;
                String a2Str = right.substring(i + 1).trim();

                int idx = s.getAttrNameIdx(attrName);
                if(s.getIthAttrType(idx).equals("INT")){
                    a2 = Long.parseLong(a2Str);
                }else if(s.getIthAttrType(idx).contains("FLOAT")){
                    int magnification = (int) Math.pow(10, s.getIthDecimalLens(idx));
                    a2 = (long) (Float.parseFloat(a2Str) * magnification);
                }else if(s.getIthAttrType(idx).contains("Double")){
                    int magnification = (int) Math.pow(10, s.getIthDecimalLens(idx));
                    a2 = (long) (Double.parseDouble(a2Str) * magnification);
                }

                //a2 = (long) (Double.parseDouble(a2Str));
                break;
            }
        }
        //注意之前没有把m1赋值 默认是1
        if(hasMulOrDiv != -1){
            String m2Str;
            if(hasAddOrSub == -1){
                m2Str = right.substring(hasMulOrDiv + 1).trim();
            }else{
                m2Str = right.substring(hasMulOrDiv + 1, hasAddOrSub);
            }
            m2 = Double.parseDouble(m2Str);
        }
    }

    /**
     * 一定要按照顺序传过来 否则正确性无法保证
     * @param value1 varName1的值
     * @param value2 varName2的值
     * @return 是不是满足依赖谓词约束
     */
    public boolean satisfy(long value1, long value2){
        double leftValue = getLeftValue(value1);
        double rightValue = getRightValue(value2);
        switch(cmp){
            case LT:
                return (leftValue < rightValue);
            case GE:
                return (leftValue >= rightValue);
            case GT:
                return (leftValue > rightValue);
            case LE:
                return (leftValue <= rightValue);
            case EQ:
                return (leftValue == rightValue);
            default:
                System.out.println("undefine compared operator");
        }
        return false;
    }

    /**
     * 得到左边表达式的值
     * @param value1 传入的参数
     * @return 表达式值
     */
    public double getLeftValue(long value1){
        double ans = 0;
        switch(ao1){
            case MUL:
                ans = value1 * m1;
                break;
            case DIV:
                ans = value1 / m1;
        }

        switch(ao2){
            case ADD:
                ans += a1;
                break;
            case SUB:
                ans -= a1;
        }
        return ans;
    }

    /**
     * 得到表达式右边的值
     * @param value2 传入的参数
     * @return 右值
     */
    public double getRightValue(long value2){
        double ans = 0;
        switch(ao3){
            case MUL:
                ans = value2 * m2;
                break;
            case DIV:
                ans = value2 / m2;
        }

        switch(ao4){
            case ADD:
                ans += a2;
                break;
            case SUB:
                ans -= a2;
        }
        return ans;
    }

    @Override
    public void print() {
        switch(cmp){
            case GT:
                System.out.println(leftPart() + " > " + rightPart());
                break;
            case GE:
                System.out.println(leftPart() + " >= " + rightPart());
                break;
            case LE:
                System.out.println(leftPart() + " <= " + rightPart());
                break;
            case LT:
                System.out.println(leftPart() + " < " + rightPart());
                break;
        }
    }

    public String leftPart(){
        StringBuilder buff = new StringBuilder();
        buff.append(varName1).append(".").append(attrName);
        if(ao1 != ArithmeticOperator.MUL || m1 != 1){
            if(ao1 == ArithmeticOperator.MUL){
                buff.append(" * ");
            }else{
                buff.append(" / ");
            }
            buff.append(m1);
        }

        if(ao2 != ArithmeticOperator.ADD || a1 != 0){
            if(ao2 == ArithmeticOperator.ADD){
                buff.append(" + ");
            }else{
                buff.append(" - ");
            }

            buff.append(a1);
        }
        return buff.toString();
    }

    public String rightPart(){
        StringBuilder buff = new StringBuilder();
        buff.append(varName2).append(".").append(attrName);
        if(ao3 != ArithmeticOperator.MUL || m2 != 1){
            if(ao3 == ArithmeticOperator.MUL){
                buff.append(" * ");
            }else{
                buff.append(" / ");
            }
            buff.append(m2);
        }

        if(ao4 != ArithmeticOperator.ADD || a2 != 0){
            if(ao4 == ArithmeticOperator.ADD){
                buff.append(" + ");
            }else{
                buff.append(" - ");
            }

            buff.append(a2);
        }
        return buff.toString();
    }

    /**
     * 测试这个类的正确性
     * @param args 空
     */
    public static void main(String[] args){
        DependentConstraint dc0= new DependentConstraint("open", "a", "b", ComparedOperator.LE);
        dc0.print();

        String str1 = "a.open + 4 >= b.open * 5";
        DependentConstraint dc1 = new DependentConstraint();
        for(int i = 0; i < str1.length(); ++i){
            if(str1.charAt(i) == '<' && str1.charAt(i + 1) == '='){
                dc1.serCmp(ComparedOperator.LE);
                dc1.constructLeft(str1.substring(0, i), null);
                dc1.constructRight(str1.substring(i + 2), null);
                break;
            }else if(str1.charAt(i) == '<'){
                dc1.serCmp(ComparedOperator.LT);
                dc1.constructLeft(str1.substring(0, i), null);
                dc1.constructRight(str1.substring(i + 1), null);
                break;
            }else if(str1.charAt(i) == '>' && str1.charAt(i + 1) == '='){
                dc1.serCmp(ComparedOperator.GE);
                dc1.constructLeft(str1.substring(0, i), null);
                dc1.constructRight(str1.substring(i + 2), null);
                break;
            }else if(str1.charAt(i) == '>'){
                dc1.serCmp(ComparedOperator.GT);
                dc1.constructLeft(str1.substring(0, i), null);
                dc1.constructRight(str1.substring(i + 1), null);
                break;
            }
        }
        dc1.print();

        String str2 = "a.open / 2 - 40 >= b.open * 5 - 1";
        DependentConstraint dc2 = new DependentConstraint();

        for(int i = 0; i < str2.length(); ++i){
            if(str2.charAt(i) == '<' && str2.charAt(i + 1) == '='){
                dc2.serCmp(ComparedOperator.LE);
                dc2.constructLeft(str2.substring(0, i), null);
                dc2.constructRight(str2.substring(i + 2), null);
                break;
            }else if(str2.charAt(i) == '<'){
                dc2.serCmp(ComparedOperator.LT);
                dc2.constructLeft(str2.substring(0, i), null);
                dc2.constructRight(str2.substring(i + 1), null);
                break;
            }else if(str2.charAt(i) == '>' && str2.charAt(i + 1) == '='){
                dc2.serCmp(ComparedOperator.GE);
                dc2.constructLeft(str2.substring(0, i), null);
                dc2.constructRight(str2.substring(i + 2), null);
                break;
            }else if(str2.charAt(i) == '>'){
                dc2.serCmp(ComparedOperator.GT);
                dc2.constructLeft(str2.substring(0, i), null);
                dc2.constructRight(str2.substring(i + 1), null);
                break;
            }
        }
        dc2.print();

        String str3 = "a.open + 3 <= b.open";
        DependentConstraint dc3 = new DependentConstraint();
        for(int i = 0; i < str3.length(); ++i){
            if(str3.charAt(i) == '<' && str3.charAt(i + 1) == '='){
                dc3.serCmp(ComparedOperator.LE);
                dc3.constructLeft(str3.substring(0, i), null);
                dc3.constructRight(str3.substring(i + 2), null);
                break;
            }else if(str3.charAt(i) == '<'){
                dc3.serCmp(ComparedOperator.LT);
                dc3.constructLeft(str3.substring(0, i), null);
                dc3.constructRight(str3.substring(i + 1), null);
                break;
            }else if(str3.charAt(i) == '>' && str3.charAt(i + 1) == '='){
                dc3.serCmp(ComparedOperator.GE);
                dc3.constructLeft(str3.substring(0, i), null);
                dc3.constructRight(str3.substring(i + 2), null);
                break;
            }else if(str3.charAt(i) == '>'){
                dc3.serCmp(ComparedOperator.GT);
                dc3.constructLeft(str3.substring(0, i), null);
                dc3.constructRight(str3.substring(i + 1), null);
                break;
            }
        }
        dc3.print();
    }
}
