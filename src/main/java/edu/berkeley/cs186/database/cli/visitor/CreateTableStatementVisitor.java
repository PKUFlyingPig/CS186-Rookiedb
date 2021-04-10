package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.ASTColumnDef;
import edu.berkeley.cs186.database.cli.parser.ASTIdentifier;
import edu.berkeley.cs186.database.cli.parser.ASTSelectStatement;
import edu.berkeley.cs186.database.cli.parser.Token;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CreateTableStatementVisitor extends StatementVisitor {
    public String tableName;
    public List<String> errorMessages = new ArrayList<>();
    public Schema schema = new Schema();
    public SelectStatementVisitor selectStatementVisitor = null;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTSelectStatement node, Object data) {
        selectStatementVisitor = new SelectStatementVisitor();
        node.jjtAccept(selectStatementVisitor, data);
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

    public void execute(Transaction transaction) {
        // transaction
        if (this.errorMessages.size() > 0) {
            for(String msg: errorMessages) {
                System.out.println(msg);
            }
            System.out.println("Failed to execute CREATE TABLE.");
        } else {
            if (selectStatementVisitor != null) {
                QueryPlan p = selectStatementVisitor.getQueryPlan(transaction).get();
                p.execute();
                QueryOperator op = p.getFinalOperator();
                Schema s = op.getSchema();
                for (int i = 0; i < s.size(); i++) {
                    if (s.getFieldName(i).contains(".")) {
                        throw new UnsupportedOperationException("Cannot have `.` in table field name.");
                    }
                }
                transaction.createTable(s, this.tableName);
                Iterator<Record> records = op.iterator();
                while (records.hasNext()) {
                    Record r = records.next();
                    transaction.insert(this.tableName, r);
                }
            } else {
                transaction.createTable(this.schema, this.tableName);
            }
            System.out.println("CREATE TABLE " + tableName);
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.CREATE_TABLE;
    }
}