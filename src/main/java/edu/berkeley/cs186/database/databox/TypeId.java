package edu.berkeley.cs186.database.databox;

public enum TypeId {
    BOOL,
    INT,
    FLOAT,
    STRING,
    LONG;

    private static final TypeId[] values = TypeId.values();

    public static TypeId fromInt(int x) {
        if (x < 0 || x >= values.length) {
            String err = String.format("Unknown TypeId ordinal %d.", x);
            throw new IllegalArgumentException(err);
        }
        return values[x];
    }
}
