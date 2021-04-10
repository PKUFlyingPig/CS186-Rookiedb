package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.ASTExpression;
import edu.berkeley.cs186.database.cli.parser.ASTIdentifier;
import edu.berkeley.cs186.database.query.aggr.DataFunction;
import edu.berkeley.cs186.database.table.Schema;

public class DeleteStatementVisitor extends StatementVisitor {
    public String tableName;
    public DataFunction cond;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTExpression node, Object data) {
        ExpressionVisitor visitor = new ExpressionVisitor();
        node.jjtAccept(visitor, data);
        this.cond = visitor.build();
    }

    @Override
    public void execute(Transaction transaction) {
        try {
            Schema schema = transaction.getSchema(tableName);
            this.cond.setSchema(schema);
            transaction.delete(tableName, cond::evaluate);
            System.out.println("DELETE");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Failed to execute DELETE.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.DELETE;
    }
}