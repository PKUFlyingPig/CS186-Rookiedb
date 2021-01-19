package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.*;
import edu.berkeley.cs186.database.common.PredicateOperator;

public class DeleteStatementVisitor extends StatementVisitor {
    public String tableName;
    public String predColumnName;
    public PredicateOperator predOperator;
    public DataBox predValue;

    @Override
    public void visit(ASTTableName node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTBinaryExpression node, Object data) {
        Object[] components = (Object[]) node.jjtGetValue();
        Object colComp = components[0];
        Object opComp = components[1];
        Object valComp = components[2];
        PredicateOperator op = PredicateOperator.fromSymbol((String) opComp);
        if (!(components[0] instanceof ASTColumnName)) {
            colComp = components[2];
            valComp = components[0];
            op = op.reverse();
        }
        String col = (String) ((ASTColumnName) colComp).jjtGetValue();
        DataBox val = PrettyPrinter.parseLiteral((String) valComp);
        this.predColumnName = col;
        this.predOperator = op;
        this.predValue = val;
    }

    @Override
    public void execute(Transaction transaction) {
        try {
            transaction.delete(tableName, predColumnName, predOperator, predValue);
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