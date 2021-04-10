package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.ASTColumnName;
import edu.berkeley.cs186.database.cli.parser.ASTExpression;
import edu.berkeley.cs186.database.cli.parser.ASTIdentifier;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.query.aggr.DataFunction;
import edu.berkeley.cs186.database.table.Schema;

public class UpdateStatementVisitor extends StatementVisitor {
    public String tableName;
    public String updateColumnName;
    public ASTExpression expr;
    public ASTExpression cond;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.updateColumnName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTExpression node, Object data) {
        if (this.expr != null) this.cond = node;
        else this.expr = node;
    }

    @Override
    public void execute(Transaction transaction) {
        try {
            Schema schema = transaction.getSchema(tableName);
            ExpressionVisitor ev = new ExpressionVisitor();
            this.expr.jjtAccept(ev, schema);
            DataFunction exprFunc = ev.build();
            DataFunction condFunc;
            if (this.cond == null) {
                condFunc = new DataFunction.LiteralDataSource(new BoolDataBox(true));
            } else {
                ExpressionVisitor condEv = new ExpressionVisitor();
                this.cond.jjtAccept(condEv, schema);
                condFunc = condEv.build();
            }
            exprFunc.setSchema(schema);
            condFunc.setSchema(schema);
            transaction.update(
                    this.tableName,
                    this.updateColumnName,
                    exprFunc::evaluate,
                    condFunc::evaluate
            );
            System.out.println("UPDATE");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to execute UPDATE.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.UPDATE;
    }
}
