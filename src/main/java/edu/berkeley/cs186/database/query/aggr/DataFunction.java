package edu.berkeley.cs186.database.query.aggr;

import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public abstract class DataFunction {
    protected boolean hasAgg = false;
    protected Schema schema = null;
    protected Set<String> dependencies = new HashSet<>();
    protected List<DataFunction> children;

    public DataFunction(DataFunction... children) {
        this.children = Arrays.asList(children);
        for (DataFunction child: children) {
            hasAgg |= child.hasAgg();
            this.dependencies.addAll(child.dependencies);
        }
    }

    public boolean hasAgg() {
        return this.hasAgg;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
        for (DataFunction child: children) child.setSchema(schema);
    }

    public void update(Record r) {
        assert (this.schema != null);
        for (DataFunction child: children) {
            if (child.hasAgg()) child.update(r);
        }
    }

    public void reset() {
        for (DataFunction child: children) {
            if (child.hasAgg()) child.reset();
        }
    }

    public Set<String> getDependencies() {
        return dependencies;
    }

    public abstract Type getType();
    public abstract DataBox evaluate(Record record);

    public static DataFunction lookupOp(String op, DataFunction a, DataFunction b) {
        op = op.toUpperCase();
        switch(op) {
            case "+": return new AddFunction(a, b);
            case "-": return new SubtractFunction(a, b);
            case "*": return new MultiplyFunction(a, b);
            case "/": return new DivisionFunction(a, b);
            case "%": return new ModuloFunction(a, b);
            case "AND": return new AndFunction(a, b);
            case "OR": return new OrFunction(a, b);
            case "=":
            case "==": return new EqualFunction(a, b);
            case "!=":
            case "<>": return new UnequalFunction(a, b);
            case ">=": return new GreaterThanEqualFunction(a, b);
            case ">": return new GreaterThanFunction(a, b);
            case "<=": return new LessThanEqualFunction(a, b);
            case "<": return new LessThanFunction(a, b);
        }
        throw new UnsupportedOperationException("Unknown operator `" + op + "`");
    }

    public static DataFunction lookupAggregate(String name, DataFunction child) {
        name = name.toUpperCase().trim();
        switch (name) {
            case "FIRST": return new AggregateDataFunction((Type t) -> new FirstAggregateFunction(t), child);
            case "SUM": return new AggregateDataFunction((Type t) -> new SumAggregateFunction(t), child);
            case "COUNT": return new AggregateDataFunction((Type t) -> new CountAggregateFunction(), child);
            case "MAX": return new AggregateDataFunction((Type t) -> new MaxAggregateFunction(t), child);
            case "MIN": return new AggregateDataFunction((Type t) -> new MinAggregateFunction(t), child);
            case "AVG": return new AggregateDataFunction((Type t) -> new AverageAggregateFunction(t), child);
            case "VARIANCE": return new AggregateDataFunction((Type t) -> new VarianceAggregateFunction(t), child);
            case "STDDEV": return new AggregateDataFunction((Type t) -> new StdDevAggregateFunction(t), child);
            case "RANGE": return new AggregateDataFunction((Type t) -> new RangeAggregateFunction(t), child);
            case "RANDOM": return new AggregateDataFunction((Type t) -> new RandomAggregateFunction(t), child);
            case "LAST": return new AggregateDataFunction((Type t) -> new LastAggregateFunction(t), child);
        }
        return null;
    }

    public static DataFunction customFunction(String name, DataFunction... children) {
        name = name.toUpperCase().trim();
        switch (name) {
            case "NEGATE": return new NegationFunction(children);
            case "UPPER": return new UpperFunction(children);
            case "LOWER": return new LowerFunction(children);
            case "REPLACE": return new ReplaceFunction(children);
            case "ROUND": return new RoundFunction(children);
            case "CEIL": return new CeilFunction(children);
            case "FLOOR": return new FloorFunction(children);
        }
        return null;
    }

    public static class AggregateDataFunction extends DataFunction {
        Function<Type, AggregateFunction> partialConstructor;
        AggregateFunction inner = null;

        public AggregateDataFunction(Function<Type, AggregateFunction> partialConstructor, DataFunction... children) {
            super(children);
            this.partialConstructor = partialConstructor;
            if (this.children.size() != 1) throw new UnsupportedOperationException("Aggregates take exactly one argument.");
            if (this.hasAgg) throw new UnsupportedOperationException("Cannot compute nested aggregate functions.");
            this.hasAgg = true;
        }

        @Override
        public void setSchema(Schema s) {
            super.setSchema(s);
            this.inner = partialConstructor.apply(children.get(0).getType());
        }

        @Override
        public void update(Record record) {
            this.inner.update(children.get(0).evaluate(record));
        }

        @Override
        public void reset() {
            this.inner.reset();
        }

        @Override
        public Type getType() {
            return this.inner.getResultType();
        }

        @Override
        public DataBox evaluate(Record record) {
            return this.inner.getResult();
        }
    }

    public static class ColumnDataSource extends DataFunction  {
        private String columnName;
        private Integer col;

        public ColumnDataSource(String columnName) {
            this.columnName = columnName;
            this.dependencies.add(columnName);
            this.col = null;
        }

        @Override
        public void setSchema(Schema schema) {
            super.setSchema(schema);
            this.col = schema.findField(this.columnName);
        }

        @Override
        public Type getType() {
            return schema.getFieldType(this.col);
        }

        @Override
        public DataBox evaluate(Record record) {
            return record.getValue(this.col);
        }
    }

    public static class LiteralDataSource extends DataFunction {
        private DataBox data;

        public LiteralDataSource(DataBox data) {
            super();
            this.data = data;
        }

        @Override
        public Type getType() {
            return data.type();
        }

        @Override
        public DataBox evaluate(Record record) {
            return data;
        }
    }

    private static int toInt(DataBox d) {
        switch (d.getTypeId()) {
            case BOOL: return d.getBool() ? 1 : 0;
            case INT: return d.getInt();
        }
        throw new UnsupportedOperationException("Cannot cast type `" + d.getTypeId() + "` to INT");
    }

    public static long toLong(DataBox d) {
        switch(d.getTypeId()) {
            case LONG: return d.getLong();
            case BOOL: return d.getBool() ? 1 : 0;
            case INT: return d.getInt();
        }
        throw new UnsupportedOperationException("Cannot cast type `" + d.getTypeId() + "` to LONG");
    }

    public static float toFloat(DataBox d) {
        switch(d.getTypeId()) {
            case FLOAT: return d.getFloat();
            case LONG: return d.getLong();
            case BOOL: return d.getBool() ? 1 : 0;
            case INT: return d.getInt();
        }
        throw new UnsupportedOperationException("Cannot cast type `" + d.getTypeId() + "` to INT");
    }

    public static boolean toBool(DataBox d) {
        switch(d.getTypeId()) {
            case BOOL: return d.getBool();
            case LONG: return d.getLong() != 0;
            case INT: return d.getInt() != 0;
            case STRING: return !d.getString().equals("");
            case FLOAT: return d.getFloat() != 0.0;
            case BYTE_ARRAY: throw new UnsupportedOperationException("Cannot interpret byte array as true/false");
            default: throw new RuntimeException("Unreachable code");
        }
    }

    public static class AddFunction extends DataFunction {
        public AddFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            Type lType = children.get(0).getType();
            Type rType = children.get(1).getType();
            if (lType.getTypeId() == TypeId.STRING || rType.getTypeId() == TypeId.STRING)
                throw new UnsupportedOperationException("+ not defined for type STRING");
            if (lType.getTypeId() == TypeId.BYTE_ARRAY || rType.getTypeId() == TypeId.BYTE_ARRAY)
                throw new UnsupportedOperationException("+ not defined for type BYTE_ARRAY");
            if (lType.getTypeId() == TypeId.FLOAT || rType.getTypeId() == TypeId.FLOAT)
                return Type.floatType();
            if (lType.getTypeId() == TypeId.LONG || rType.getTypeId() == TypeId.LONG)
                return Type.longType();
            return Type.intType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            switch (getType().getTypeId()) {
                case FLOAT: return new FloatDataBox(toFloat(left) + toFloat(right));
                case LONG: return new LongDataBox(toLong(left) + toLong(right));
                case INT: return new IntDataBox(toInt(left) + toInt(right));
            }
            throw new RuntimeException("Unreachable code.");
        }
    }

    public static class SubtractFunction extends DataFunction {
        public SubtractFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            Type lType = children.get(0).getType();
            Type rType = children.get(1).getType();
            if (lType.getTypeId() == TypeId.STRING || rType.getTypeId() == TypeId.STRING)
                throw new UnsupportedOperationException("- not defined for type STRING");
            if (lType.getTypeId() == TypeId.BYTE_ARRAY || rType.getTypeId() == TypeId.BYTE_ARRAY)
                throw new UnsupportedOperationException("- not defined for type BYTE_ARRAY");
            if (lType.getTypeId() == TypeId.FLOAT || rType.getTypeId() == TypeId.FLOAT)
                return Type.floatType();
            if (lType.getTypeId() == TypeId.LONG || rType.getTypeId() == TypeId.LONG)
                return Type.longType();
            return Type.intType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            switch (getType().getTypeId()) {
                case FLOAT: return new FloatDataBox(toFloat(left) - toFloat(right));
                case LONG: return new LongDataBox(toLong(left) - toLong(right));
                case INT: return new IntDataBox(toInt(left) - toInt(right));
            }
            throw new RuntimeException("Unreachable code.");
        }
    }

    public static class MultiplyFunction extends DataFunction {
        public MultiplyFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            Type lType = children.get(0).getType();
            Type rType = children.get(1).getType();
            if (lType.getTypeId() == TypeId.STRING || rType.getTypeId() == TypeId.STRING)
                throw new UnsupportedOperationException("* not defined for type STRING");
            if (lType.getTypeId() == TypeId.BYTE_ARRAY || rType.getTypeId() == TypeId.BYTE_ARRAY)
                throw new UnsupportedOperationException("* not defined for type BYTE_ARRAY");
            if (lType.getTypeId() == TypeId.FLOAT || rType.getTypeId() == TypeId.FLOAT)
                return Type.floatType();
            if (lType.getTypeId() == TypeId.LONG || rType.getTypeId() == TypeId.LONG)
                return Type.longType();
            return Type.intType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            switch (getType().getTypeId()) {
                case FLOAT: return new FloatDataBox(toFloat(left) * toFloat(right));
                case LONG: return new LongDataBox(toLong(left) * toLong(right));
                case INT: return new IntDataBox(toInt(left) * toInt(right));
            }
            throw new RuntimeException("Unreachable code.");
        }
    }

    public static class ModuloFunction extends DataFunction {
        public ModuloFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            Type lType = children.get(0).getType();
            Type rType = children.get(1).getType();
            if (lType.getTypeId() == TypeId.STRING || rType.getTypeId() == TypeId.STRING)
                throw new UnsupportedOperationException("% not defined for type STRING");
            if (lType.getTypeId() == TypeId.BYTE_ARRAY || rType.getTypeId() == TypeId.BYTE_ARRAY)
                throw new UnsupportedOperationException("% not defined for type BYTE_ARRAY");
            if (lType.getTypeId() == TypeId.FLOAT || rType.getTypeId() == TypeId.FLOAT)
                return Type.floatType();
            if (lType.getTypeId() == TypeId.LONG || rType.getTypeId() == TypeId.LONG)
                return Type.longType();
            return Type.intType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            switch (getType().getTypeId()) {
                case FLOAT: return new FloatDataBox(toFloat(left) % toFloat(right));
                case LONG: return new LongDataBox(toLong(left) % toLong(right));
                case INT: return new IntDataBox(toInt(left) % toInt(right));
            }
            throw new RuntimeException("Unreachable code.");
        }
    }

    public static class DivisionFunction extends DataFunction {
        public DivisionFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            Type lType = children.get(0).getType();
            Type rType = children.get(1).getType();
            if (lType.getTypeId() == TypeId.STRING || rType.getTypeId() == TypeId.STRING)
                throw new UnsupportedOperationException("/ not defined for type STRING");
            if (lType.getTypeId() == TypeId.BYTE_ARRAY || rType.getTypeId() == TypeId.BYTE_ARRAY)
                throw new UnsupportedOperationException("/ not defined for type BYTE_ARRAY");
            if (lType.getTypeId() == TypeId.FLOAT || rType.getTypeId() == TypeId.FLOAT)
                return Type.floatType();
            if (lType.getTypeId() == TypeId.LONG || rType.getTypeId() == TypeId.LONG)
                return Type.longType();
            return Type.intType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            switch (getType().getTypeId()) {
                case FLOAT: return new FloatDataBox(toFloat(left) / toFloat(right));
                case LONG: return new LongDataBox(toLong(left) / toLong(right));
                case INT: return new IntDataBox(toInt(left) / toInt(right));
            }
            throw new RuntimeException("Unreachable code.");
        }
    }

    public static class AndFunction extends DataFunction {
        public AndFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            return Type.boolType();
        }

        @Override
        public DataBox evaluate(Record record) {
            return new BoolDataBox(
                toBool(children.get(0).evaluate(record)) &&
                toBool(children.get(1).evaluate(record))
            );
        }
    }

    public static class OrFunction extends DataFunction {
        public OrFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            return Type.boolType();
        }

        @Override
        public DataBox evaluate(Record record) {
            return new BoolDataBox(
                toBool(children.get(0).evaluate(record)) ||
                toBool(children.get(1).evaluate(record))
            );
        }
    }

    public static class LessThanFunction extends DataFunction {
        public LessThanFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            return Type.boolType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            return new BoolDataBox(left.compareTo(right) < 0);
        }
    }

    public static class LessThanEqualFunction extends DataFunction {
        public LessThanEqualFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            return Type.boolType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            return new BoolDataBox(left.compareTo(right) <= 0);
        }
    }

    public static class GreaterThanFunction extends DataFunction {
        public GreaterThanFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            return Type.boolType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            return new BoolDataBox(left.compareTo(right) > 0);
        }
    }

    public static class GreaterThanEqualFunction extends DataFunction {
        public GreaterThanEqualFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            return Type.boolType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            return new BoolDataBox(left.compareTo(right) >= 0);
        }
    }

    public static class EqualFunction extends DataFunction {
        public EqualFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            return Type.boolType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            return new BoolDataBox(left.compareTo(right) == 0);
        }
    }

    public static class UnequalFunction extends DataFunction {
        public UnequalFunction(DataFunction a, DataFunction b) {
            super(new DataFunction[]{a, b});
        }

        @Override
        public Type getType() {
            return Type.boolType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox left = children.get(0).evaluate(record);
            DataBox right = children.get(1).evaluate(record);
            return new BoolDataBox(left.compareTo(right) != 0);
        }
    }

    public static class NegationFunction extends DataFunction {
        public NegationFunction(DataFunction... children) {
            super(children);
            if (children.length != 1) {
                throw new UnsupportedOperationException("Negation takes exactly one argument");
            }
        }

        @Override
        public Type getType() {
            switch (children.get(0).getType().getTypeId()) {
                case FLOAT: return Type.floatType();
                case INT: return Type.intType();
                case LONG: return Type.longType();
                default:
                    throw new UnsupportedOperationException("Can't negate data of type `" + children.get(0).getType() + "`");
            }
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox value = this.children.get(0).evaluate(record);
            switch (value.getTypeId()) {
                case FLOAT:
                    return new FloatDataBox(value.getFloat() * -1);
                case INT:
                    return new IntDataBox(value.getInt() * -1);
                case LONG:
                    return new LongDataBox(value.getLong() * -1);
                default:
                    throw new UnsupportedOperationException("Can't negate data of type `" + value.getTypeId() + "`");
            }
        }
    }

    public static class UpperFunction extends DataFunction {
        public UpperFunction(DataFunction... children) {
            super(children);
            if (children.length != 1) {
                throw new UnsupportedOperationException("UPPER takes exactly one argument");
            }
        }

        @Override
        public Type getType() {
            DataFunction f = this.children.get(0);
            if (f.getType().getTypeId() != TypeId.STRING) {
                throw new UnsupportedOperationException("UPPER can only be used on strings.");
            }
            return f.getType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox value = this.children.get(0).evaluate(record);
            return new StringDataBox(value.getString().toUpperCase());
        }
    }

    public static class LowerFunction extends DataFunction {
        public LowerFunction(DataFunction... children) {
            super(children);
            if (children.length != 1) {
                throw new UnsupportedOperationException("LOWER takes exactly one argument");
            }
        }

        @Override
        public Type getType() {
            DataFunction f = this.children.get(0);
            if (f.getType().getTypeId() != TypeId.STRING) {
                throw new UnsupportedOperationException("LOWER can only be used on strings.");
            }
            return f.getType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox value = this.children.get(0).evaluate(record);
            return new StringDataBox(value.getString().toLowerCase());
        }
    }

    public static class ReplaceFunction extends DataFunction {
        public ReplaceFunction(DataFunction... children) {
            super(children);
            if (children.length != 3) {
                throw new UnsupportedOperationException("REPLACE takes exactly three arguments");
            }
        }

        @Override
        public Type getType() {
            DataFunction f1 = this.children.get(0);
            DataFunction f2 = this.children.get(1);
            DataFunction f3 = this.children.get(1);
            if (f1.getType().getTypeId() != TypeId.STRING ||
                f2.getType().getTypeId() != TypeId.STRING ||
                f3.getType().getTypeId() != TypeId.STRING) {
                throw new UnsupportedOperationException("All arguments of REPLACE must be of type STRING");
            }
            return f1.getType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox v1 = this.children.get(0).evaluate(record);
            DataBox v2 = this.children.get(1).evaluate(record);
            DataBox v3 = this.children.get(2).evaluate(record);
            return new StringDataBox(v1.getString().replace(v2.getString(), v3.getString()));
        }
    }

    public static class FloorFunction extends DataFunction {
        public FloorFunction(DataFunction... children) {
            super(children);
            if (children.length != 1) {
                throw new UnsupportedOperationException("FLOOR takes exactly one argument");
            }
        }

        @Override
        public Type getType() {
            TypeId t = this.children.get(0).getType().getTypeId();
            if (t == TypeId.STRING || t == TypeId.BYTE_ARRAY) {
                throw new UnsupportedOperationException("FLOOR is not defined for types STRING and BYTE_ARRAY");
            }
            return Type.longType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox v = this.children.get(0).evaluate(record);
            return new LongDataBox(Math.round(Math.floor(toFloat(v))));
        }
    }

    public static class CeilFunction extends DataFunction {
        public CeilFunction(DataFunction... children) {
            super(children);
            if (children.length != 1) {
                throw new UnsupportedOperationException("CEIL takes exactly one argument");
            }
        }

        @Override
        public Type getType() {
            TypeId t = this.children.get(0).getType().getTypeId();
            if (t == TypeId.STRING || t == TypeId.BYTE_ARRAY) {
                throw new UnsupportedOperationException("CEIL is not defined for types STRING and BYTE_ARRAY");
            }
            return Type.longType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox v = this.children.get(0).evaluate(record);
            return new LongDataBox(Math.round(Math.ceil(toFloat(v))));
        }
    }

    public static class RoundFunction extends DataFunction {
        public RoundFunction(DataFunction... children) {
            super(children);
            if (children.length != 1) {
                throw new UnsupportedOperationException("ROUND takes exactly one argument");
            }
        }

        @Override
        public Type getType() {
            TypeId t = this.children.get(0).getType().getTypeId();
            if (t == TypeId.STRING || t == TypeId.BYTE_ARRAY) {
                throw new UnsupportedOperationException("ROUND is not defined for types STRING and BYTE_ARRAY");
            }
            return Type.longType();
        }

        @Override
        public DataBox evaluate(Record record) {
            DataBox v = this.children.get(0).evaluate(record);
            return new LongDataBox(Math.round(toFloat(v)));
        }
    }
}
