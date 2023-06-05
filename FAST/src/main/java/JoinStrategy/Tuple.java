package JoinStrategy;

import java.util.ArrayList;
import java.util.List;

/**
 * Matched String tuple
 */
public class Tuple {
    private final List<String> fullMatch;

    public Tuple(int size){
        fullMatch = new ArrayList<>(size);
    }

    public void addEvent(String eventRecord){
        fullMatch.add(eventRecord);
    }

    public void addAllEvents(List<String> records){
        fullMatch.addAll(records);
    }

    @Override
    public String toString(){
        StringBuilder ans = new StringBuilder(64);
        for(String record: fullMatch){
            ans.append(record).append(" ");
        }
        return ans.toString();
    }
}
