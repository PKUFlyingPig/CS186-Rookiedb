package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.ASTColumnName;
import edu.berkeley.cs186.database.cli.parser.ASTComparisonOperator;
import edu.berkeley.cs186.database.cli.parser.ASTLiteral;
import edu.berkeley.cs186.database.cli.parser.RookieParserDefaultVisitor;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;

public class ColumnValueComparisonVisitor extends RookieParserDefaultVisitor {
    PredicateOperator op;
    String columnName;
    DataBox value;

    @Override
    public void visit(ASTLiteral node, Object data) {
        this.value = PrettyPrinter.parseLiteral((String) node.jjtGetValue());
    }

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.columnName = (String) node.jjtGetValue();
        // keep things in format columnName <= value
        if (this.op != null) this.op = op.reverse();
    }

    @Override
    public void visit(ASTComparisonOperator node, Object data) {
        this.op = PredicateOperator.fromSymbol((String) node.jjtGetValue());
    }
}
