package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.query.TestSourceOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.List;

public class TestUtils {
    public static Schema createSchemaWithAllTypes() {
        return new Schema()
                .add("bool", Type.boolType())
                .add("int", Type.intType())
                .add("string", Type.stringType(1))
                .add("float", Type.floatType());
    }

    public static Record createRecordWithAllTypes() {
        return new Record(true, 1, "a", 1.2f);
    }

    public static TestSourceOperator createSourceWithAllTypes(int numRecords) {
        Schema schema = createSchemaWithAllTypes();
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++)
            records.add(createRecordWithAllTypes());
        return new TestSourceOperator(records, schema);
    }

    public static Record createRecordWithAllTypesWithValue(int val) {
        return new Record(true, val, "" + (char) (val % 79 + 0x30), 1.0f);
    }

    public static TestSourceOperator createSourceWithInts(List<Integer> values) {
        Schema schema = new Schema().add("int", Type.intType());
        List<Record> recordList = new ArrayList<Record>();
        for (int v : values) recordList.add(new Record(v));
        return new TestSourceOperator(recordList, schema);
    }
}
