package edu.berkeley.cs186.database.common;

/**
 * Predicate operators represent the possible comparison's we allow in
 * our database implementation. For example, in a WHERE clause we may
 * specificy WHERE table.value >= 186. To express this we would use
 * PredicateOperator.GREATER_THAN_EQUALS. This is useful in QueryPlan.select()
 * when we're trying to add constraints to the WHERE clause of our query.
 */
public enum PredicateOperator {
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    GREATER_THAN,
    GREATER_THAN_EQUALS;

    /**
     * @param a The left argument to be evaluated
     * @param b The right argument to be evaluated
     * @param <T> Any type that implements comparable
     * @return The result of evaluating this predicate operator on the left and
     * right arguments.
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
     * @param s A string representing a predicate operator, for example >= or !=
     * @return An instance of the corresponding PredicateOperator
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
        throw new IllegalArgumentException("Invalid predicate symbol:  + s");
    }

    /**
     * @return a symbolic representation of this operator.
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
        throw new IllegalStateException("Unreachable code.");
    }

    /**
     * @param
     */

    /**
     * @return A corresponding operator to use if the left and right values
     * this operator was comparing were reversed. Does nothing for NOT_EQUALS
     * and EQUALS, and flips GREATER THAN with LESS THAN or vice versa for
     * the remaining cases.
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
