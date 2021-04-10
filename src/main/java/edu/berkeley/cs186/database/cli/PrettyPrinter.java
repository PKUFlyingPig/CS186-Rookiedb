package edu.berkeley.cs186.database.cli;

import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PrettyPrinter {
    public static void printTable(Table t) {
        Schema s = t.getSchema();
        printRecords(s.getFieldNames(), t.iterator());
    }

    public static void printSchema(Schema s) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < s.size(); i++) {
            records.add(new Record(Arrays.asList(
                new StringDataBox(s.getFieldName(i), 32),
                new StringDataBox(s.getFieldType(i).toString(), 32)
            )));
        }
        printRecords(Arrays.asList("column_name", "type"), records.iterator());
    }

    public static void printRecords(List<String> columnNames, Iterator<Record> records) {
        ArrayList<Integer> maxWidths = new ArrayList<>();
        for(String columnName: columnNames) {
            maxWidths.add(columnName.length());
        }
        ArrayList<Record> recordList = new ArrayList<Record>();
        while(records.hasNext()) {
            Record record = records.next();
            recordList.add(record);
            List<DataBox> fields = record.getValues();
            for(int i = 0; i < fields.size(); i++) {
                DataBox field = fields.get(i);
                maxWidths.set(i, Integer.max(
                    maxWidths.get(i),
                    field.toString().replace("\0", "").length()
                ));
            }
        }
        printRow(columnNames, maxWidths);
        printSeparator(maxWidths);
        for(Record record: recordList) {
            printRecord(record, maxWidths);
        }
        if (recordList.size() != 1) {
            System.out.printf("(%d rows)\n", recordList.size());
        } else {
            System.out.printf("(%d row)\n", recordList.size());
        }
    }

    private static void printRow(List<String> values, List<Integer> padding) {
        for(int i = 0; i < values.size(); i++) {
            if (i > 0) System.out.print("|");
            String curr = values.get(i);
            if (i == values.size() - 1) {
                System.out.println(" " + curr);
                break;
            }
            System.out.printf(" %-"+padding.get(i)+"s ", curr);
        }
    }

    public static void printRecord(Record record, List<Integer> padding) {
        List<String> row = new ArrayList<>();
        List<DataBox> values = record.getValues();
        for (int i = 0; i < values.size(); i++) {
            DataBox field = values.get(i);
            String cleaned = field.toString().replace("\0", "");
            if(field.type().equals(Type.longType()) || field.type().equals(Type.intType())) {
                cleaned = String.format("%" + padding.get(i) + "s", cleaned);
            }
            row.add(cleaned);
        }
        printRow(row, padding);
    }

    private static void printSeparator(List<Integer> padding) {
        for(int i = 0; i < padding.size(); i++) {
            if (i > 0) System.out.print("+");
            for(int j = 0; j < padding.get(i) + 2; j++)
                System.out.print("-");
        }
        System.out.println();
    }

    public static DataBox parseLiteral(String literal) {
        String literalLower = literal.toLowerCase();
        if(literal.charAt(0) == '\'') {
            String unescaped = literal.substring(1, literal.length() - 1);
            String escaped = unescaped.replace("''", "''");
            return new StringDataBox(escaped, escaped.length());
        } else if(literalLower.equals("true")) {
            return new BoolDataBox(true);
        } else if(literalLower.equals("false")){
            return new BoolDataBox(false);
        } if (literal.indexOf('.') != -1) {
            return new FloatDataBox(Float.parseFloat(literal));
        } else {
            return new IntDataBox(Integer.parseInt(literal));
        }
    }
}