package edu.berkeley.cs186.database.cli.visitor;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.cli.parser.*;
import edu.berkeley.cs186.database.table.Schema;

public class CreateTableStatementVisitor extends StatementVisitor {
    public String tableName;
    public List<String> errorMessages = new ArrayList<>();
    public Schema schema;

    @Override
    public void visit(ASTTableName node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTColumnDef node, Object data) {
        Object[] components = (Object[]) node.jjtGetValue();
        String fieldName = (String) components[0];
        String fieldTypeStr = (String) components[1];
        Token param = (Token) components[2];
        Type fieldType = Type.intType();
        switch(fieldTypeStr.toLowerCase()) {
            case "int":;
            case "integer":
                fieldType = Type.intType();
                break;
            case "char":;
            case "varchar":;
            case "string":
                if(param == null) {
                    errorMessages.add(String.format("Missing length for %s(n).", fieldType));
                    return;
                }
                String s = param.image;
                if (s.indexOf('.') >= 0) {
                    errorMessages.add(String.format("Length of %s(n) must be integer, not `%s`.", fieldType, s));
                    return;
                }
                fieldType = Type.stringType(Integer.parseInt(s));
                break;
            case "float":
                fieldType = Type.floatType();
                break;
            case "long":
                fieldType = Type.longType();
                break;
            case "bool":;
            case "boolean":
                fieldType = Type.boolType();
                break;
            default:
                assert false: String.format(
                    "Invalid field type \"%s\"",
                    fieldTypeStr
                );
        }
        schema.add(fieldName, fieldType);
    }

    public void prettyPrint() {
        System.out.println("CREATE TABLE " + this.tableName + "(");
        for(int i = 0; i < schema.size(); i++) {
            if (i > 0) System.out.println(",");
            System.out.print("   " + schema.getFieldName(i) + " " + schema.getFieldType(i));
        }
        System.out.println("\n)");
    }

    public void execute(Transaction transaction) {
        // transaction
        if (this.errorMessages.size() > 0) {
            for(String msg: errorMessages) {
                System.out.println(msg);
            }
            System.out.println("Failed to execute CREATE TABLE.");
        } else {
            transaction.createTable(this.schema, this.tableName);
            System.out.println("CREATE TABLE " + tableName);
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.CREATE_TABLE;
    }
}