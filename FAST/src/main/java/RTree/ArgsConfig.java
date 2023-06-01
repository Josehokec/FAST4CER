package RTree;

/*this class include const and variables*/
public class ArgsConfig {

    int pointDimension;

    public ArgsConfig(){
        // default dimension is 2
        pointDimension = 2;
    }

    public static final int MAX_ENTRIES_IN_NODE = 20;
    public static final int MIN_ENTRIES_IN_NODE = 10;


}
