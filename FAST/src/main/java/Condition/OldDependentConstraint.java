/*
package Condition;

import Common.ArithmeticOperator;
import Common.ComparedOperator;
*/
/**
 * 该类已经被废弃
 * 由于RangeBitmap原因，constVal会统一成long格式
 * 目前只支持的依赖谓词形式如下:
 * a.open + const_value1 <= b.open + const_value2
 * a.open * const_value1 <= b.open + const_value2
 * a.open <= b.open
 * ----------------------------------------------
 * 不支持格式:
 * a.open <= b.open + c.open
 * a.open <= b.open <= c.open
 * 2 * a.open + 4 <= 4 * b.open + 9
 */
/*
public class OldDependentConstraint extends Constraint{
    String attrName;
    String varName1;
    ArithmeticOperator ao1;
    long constVal1;
    ComparedOperator op;
    String varName2;
    ArithmeticOperator ao2;
    long constVal2;

    public OldDependentConstraint(String attrName, String varName1, ComparedOperator op, String varName2) {
        this.attrName = attrName;
        this.varName1 = varName1;
        this.ao1 = ArithmeticOperator.ADD;
        this.constVal1 = 0;
        this.op = op;
        this.varName2 = varName2;
        this.ao2 = ArithmeticOperator.ADD;
        this.constVal2 = 0;
    }

    public OldDependentConstraint(String attrName, String varName1, ArithmeticOperator ao1, long constVal1,
                                  ComparedOperator op, String varName2, ArithmeticOperator ao2, long constVal2) {
        this.attrName = attrName;
        this.varName1 = varName1;
        this.ao1 = ao1;
        this.constVal1 = constVal1;
        this.op = op;
        this.varName2 = varName2;
        this.ao2 = ao2;
        this.constVal2 = constVal2;
    }

    public String getAttrName(){
        return attrName;
    }

    public String getVarName1() {
        return varName1;
    }

    public String getVarName2() {
        return varName2;
    }

    public void print(){
        System.out.print(varName1 + "." + attrName );
        boolean flag1 = (ao1 == ArithmeticOperator.ADD && constVal1 == 0);
        if(!flag1){
            switch(ao1){
                case ADD:
                    System.out.print(" + ");
                    break;
                case SUB:
                    System.out.print(" - ");
                    break;
                case DIV:
                    System.out.print(" / ");
                    break;
                case MUL:
                    System.out.print(" * ");
                    break;
            }
            System.out.print(constVal1);
        }

        switch(op){
            case LE:
                System.out.print(" <= ");
                break;
            case LT:
                System.out.print(" < ");
                break;
            case GE:
                System.out.print(" >= ");
                break;
            case GT:
                System.out.print(" > ");
                break;
        }

        System.out.print(varName2 + "." + attrName);
        boolean flag2 = (ao2 == ArithmeticOperator.ADD && constVal2 == 0);
        if(!flag2){
            switch(ao2){
                case ADD:
                    System.out.print(" + ");
                    break;
                case SUB:
                    System.out.print(" - ");
                    break;
                case DIV:
                    System.out.print(" / ");
                    break;
                case MUL:
                    System.out.print(" * ");
                    break;
            }
            System.out.print(constVal2);
        }

        System.out.println();
    }
}
*/

