package Common;

/**
 * 存储位置 实际上存startPos和offset就可以了，可以减少一次减法运算
 * @param startPos  开始位置
 * @param offset    偏移
 */
record StorePos(int startPos, int offset){}