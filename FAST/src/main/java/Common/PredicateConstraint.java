package Common;

/*
 90 <  a.open < 100
 100 < a.volume < 1000
 */
public class PredicateConstraint<T>{
    private String attributeName;
    private ComparedOperator op;
    private T value;

    public PredicateConstraint(String attributeName, ComparedOperator op, T value){
        this.attributeName = attributeName;
        this.op = op;
        this.value = value;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public ComparedOperator getOp() {
        return op;
    }

    public T getValue() {
        return value;
    }

    public void print(){
        String str = "\'" + attributeName + "\'";
        if(op == ComparedOperator.GE){
            str = str + ">=" + value;
        }else if(op == ComparedOperator.GT){
            str = str + ">" + value;
        }else if(op == ComparedOperator.LE){
            str = str + "<=" + value;
        }else if(op == ComparedOperator.LT){
            str = str + "<" + value;
        }else{
            System.out.println("No exist this operator.");
        }
        System.out.print(str);
    }

    public void println(){
        print();
        System.out.println();
    }
}