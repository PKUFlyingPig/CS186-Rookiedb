package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.ASTTableName;

public class DropTableStatementVisitor extends StatementVisitor {
    public String tableName;

    @Override
    public void visit(ASTTableName node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void execute(Transaction transaction) {
        try {
            transaction.dropTable(this.tableName);
            System.out.println("DROP TABLE " + this.tableName + ";");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Failed to execute DROP TABLE.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.DROP_TABLE;
    }

}