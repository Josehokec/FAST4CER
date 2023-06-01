package Method;

import java.util.List;

/**
 * 三元组
 */

public record SingleVarChoice(int variableId, double minCost, List<Integer> predicatePos) {
}