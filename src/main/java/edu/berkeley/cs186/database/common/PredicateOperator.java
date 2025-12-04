package edu.berkeley.cs186.database.common;

/**
 * 谓词运算符表示我们在数据库实现中允许的所有比较操作。
 * 例如，在WHERE子句中我们可能会指定WHERE table.value >= 186。
 * 为了表达这一点，我们会使用PredicateOperator.GREATER_THAN_EQUALS。
 * 这在QueryPlan.select()中很有用，当我们试图向查询的WHERE子句添加约束时。
 */
public enum PredicateOperator {
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    GREATER_THAN,
    GREATER_THAN_EQUALS;

    /**
     * @param a 需要评估的左参数
     * @param b 需要评估的右参数
     * @param <T> 实现了Comparable接口的任意类型
     * @return 对左右参数应用此谓词运算符后的评估结果
     */
    public <T extends Comparable<T>> boolean evaluate(T a, T b) {
        switch (this) {
        case EQUALS:
            return a.compareTo(b) == 0;
        case NOT_EQUALS:
            return a.compareTo(b) != 0;
        case LESS_THAN:
            return a.compareTo(b) < 0;
        case LESS_THAN_EQUALS:
            return a.compareTo(b) <= 0;
        case GREATER_THAN:
            return a.compareTo(b) > 0;
        case GREATER_THAN_EQUALS:
            return a.compareTo(b) >= 0;
        }
        return false;
    }

    /**
     * @param s 表示谓词运算符的字符串，例如 >= 或 !=
     * @return 对应的PredicateOperator实例
     */
    public static PredicateOperator fromSymbol(String s) {
        switch(s) {
            case "=":;
            case "==": return EQUALS;
            case "!=":
            case "<>": return NOT_EQUALS;
            case "<": return LESS_THAN;
            case "<=": return LESS_THAN_EQUALS;
            case ">": return GREATER_THAN;
            case ">=": return GREATER_THAN_EQUALS;
        }
        throw new IllegalArgumentException("无效的谓词符号: " + s);
    }

    /**
     * @return 此运算符的符号表示
     */
    public String toSymbol() {
        switch(this) {
            case EQUALS: return "=";
            case NOT_EQUALS: return "!=";
            case LESS_THAN: return "<";
            case LESS_THAN_EQUALS: return "<=";
            case GREATER_THAN: return ">";
            case GREATER_THAN_EQUALS: return ">=";
        }
        throw new IllegalStateException("无法到达的代码。");
    }

    /**
     * @return 如果将此运算符比较的左右值互换，则应使用的对应运算符。
     * 对NOT_EQUALS和EQUALS不执行任何操作，
     * 对其余情况则将GREATER THAN与LESS THAN相互翻转。
     */
    public PredicateOperator reverse() {
        switch(this) {
            case LESS_THAN: return GREATER_THAN;
            case LESS_THAN_EQUALS: return GREATER_THAN_EQUALS;
            case GREATER_THAN: return LESS_THAN;
            case GREATER_THAN_EQUALS: return LESS_THAN_EQUALS;
            default: return this;
        }
    }
}
