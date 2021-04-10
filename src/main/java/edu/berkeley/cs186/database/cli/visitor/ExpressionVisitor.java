package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.*;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.query.aggr.DataFunction;

import java.util.ArrayList;
import java.util.List;

public class ExpressionVisitor extends RookieParserDefaultVisitor {
    DataFunction func;

    @Override
    public void visit(ASTLogicalExpression node, Object data) {
        LogicalExpressionVisitor lev = new LogicalExpressionVisitor();
        node.jjtAccept(lev, data);
        this.func = lev.build();
    }

    public DataFunction build() {
        return this.func;
    }

    private class LogicalExpressionVisitor extends RookieParserDefaultVisitor {
        private List<DataFunction> operands = new ArrayList<>();
        private List<String> operators = new ArrayList<>();

        @Override
        public void visit(ASTLogicalOperator node, Object data) {
            this.operators.add((String) node.jjtGetValue());
        }

        @Override
        public void visit(ASTComparisonExpression node, Object data) {
            ComparisonExpressionVisitor cev = new ComparisonExpressionVisitor();
            node.jjtAccept(cev, data);
            this.operands.add(cev.build());
        }

        public DataFunction build() {
            List<DataFunction> intersections = new ArrayList<>();
            if (operands.size() == 1) return operands.get(0);
            DataFunction prev = operands.get(0);
            for (int i = 0; i < operands.size(); i++) {
                String op = operators.get(i).toUpperCase();
                DataFunction curr = operands.get(i+1);
                if (op.equals("AND")) {
                    prev = new DataFunction.AndFunction(prev, curr);
                } else {
                    intersections.add(prev);
                    prev = curr;
                }
            }
            intersections.add(prev);
            prev = new DataFunction.LiteralDataSource(new BoolDataBox(false));
            for (int i = 0; i < intersections.size(); i++) {
                prev = new DataFunction.OrFunction(prev, intersections.get(i));
            }
            return prev;
        }
    }

    private class ComparisonExpressionVisitor extends RookieParserDefaultVisitor {
        private List<DataFunction> operands = new ArrayList<>();
        private List<String> operators = new ArrayList<>();

        @Override
        public void visit(ASTComparisonOperator node, Object data) {
            this.operators.add((String) node.jjtGetValue());
        }

        @Override
        public void visit(ASTAdditiveExpression node, Object data) {
            AdditiveExpressionVisitor aev = new AdditiveExpressionVisitor();
            node.jjtAccept(aev, data);
            this.operands.add(aev.build());
        }

        public DataFunction build() {
            if (this.operands.size() == 1) return this.operands.get(0);
            DataFunction prev = this.operands.get(0);
            for (int i = 0; i < operators.size(); i++) {
                DataFunction curr = this.operands.get(i+1);
                prev = DataFunction.lookupOp(this.operators.get(i), prev, curr);
            }
            return prev;
        }
    }

    private class AdditiveExpressionVisitor extends RookieParserDefaultVisitor {
        private List<DataFunction> operands = new ArrayList<>();
        private List<String> operators = new ArrayList<>();

        @Override
        public void visit(ASTAdditiveOperator node, Object data) {
            this.operators.add((String) node.jjtGetValue());
        }

        @Override
        public void visit(ASTMultiplicativeExpression node, Object data) {
            MultiplicativeExpressionVisitor mev = new MultiplicativeExpressionVisitor();
            node.jjtAccept(mev, data);
            this.operands.add(mev.build());
        }

        public DataFunction build() {
            if (this.operands.size() == 1) return this.operands.get(0);
            DataFunction prev = this.operands.get(0);
            for (int i = 0; i < operators.size(); i++) {
                DataFunction curr = this.operands.get(i+1);
                prev = DataFunction.lookupOp(this.operators.get(i), prev, curr);
            }
            return prev;
        }
    }

    private class MultiplicativeExpressionVisitor extends RookieParserDefaultVisitor {
        private List<DataFunction> operands = new ArrayList<>();
        private List<String> operators = new ArrayList<>();

        @Override
        public void visit(ASTMultiplicativeOperator node, Object data) {
            this.operators.add((String) node.jjtGetValue());
        }

        @Override
        public void visit(ASTPrimaryExpression node, Object data) {
            PrimaryExpressionVisitor pev = new PrimaryExpressionVisitor();
            node.jjtAccept(pev, data);
            this.operands.add(pev.build());
        }

        public DataFunction build() {
            if (this.operands.size() == 1) return this.operands.get(0);
            DataFunction prev = this.operands.get(0);
            for (int i = 0; i < operators.size(); i++) {
                DataFunction curr = this.operands.get(i+1);
                prev = DataFunction.lookupOp(this.operators.get(i), prev, curr);
            }
            return prev;
        }
    }

    private class PrimaryExpressionVisitor extends RookieParserDefaultVisitor {
        private boolean negated = false;
        boolean seenRoot = false;
        private DataFunction childFunc;

        @Override
        public void visit(ASTPrimaryExpression node, Object data) {
            if (!seenRoot) {
                seenRoot = true;
                super.visit(node, data);
            } else {
                PrimaryExpressionVisitor pev = new PrimaryExpressionVisitor();
                node.jjtAccept(pev, data);
                this.childFunc = pev.build();
                if (negated) {
                    this.childFunc = new DataFunction.NegationFunction(pev.build());
                }
            }
        }

        @Override
        public void visit(ASTExpression node, Object data) {
            ExpressionVisitor ev = new ExpressionVisitor();
            node.jjtAccept(ev, data);
            this.childFunc = ev.build();
        }

        @Override
        public void visit(ASTLiteral node, Object data) {
            DataBox value = PrettyPrinter.parseLiteral((String) node.jjtGetValue());
            this.childFunc = new DataFunction.LiteralDataSource(value);
        }

        @Override
        public void visit(ASTColumnName node, Object data) {
            String columnName = (String) node.jjtGetValue();
            this.childFunc = new DataFunction.ColumnDataSource(columnName);
        }

        @Override
        public void visit(ASTAdditiveOperator node, Object data) {
            String symbol = (String) node.jjtGetValue();
            if (symbol == "-") this.negated ^= true;
        }

        @Override
        public void visit(ASTFunctionCallExpression node, Object data) {
            FunctionCallVisitor fcv = new FunctionCallVisitor();
            node.jjtAccept(fcv, data);
            this.childFunc = fcv.build();
        }

        public DataFunction build() {
            return this.childFunc;
        }
    }

    private class FunctionCallVisitor extends RookieParserDefaultVisitor {
        String functionName;
        List<DataFunction> operands = new ArrayList<>();

        @Override
        public void visit(ASTIdentifier node, Object data) {
            functionName = (String) node.jjtGetValue();
        }

        @Override
        public void visit(ASTExpression node, Object data) {
            ExpressionVisitor ev = new ExpressionVisitor();
            node.jjtAccept(ev, data);
            this.operands.add(ev.build());
        }

        public DataFunction build() {
            if (this.operands.size() == 1 || (this.functionName.toUpperCase().equals("COUNT"))) {
                DataFunction child;
                if (this.operands.size() == 1) child = this.operands.get(0);
                else child = new DataFunction.LiteralDataSource(new IntDataBox(0));
                DataFunction agg = DataFunction.lookupAggregate(this.functionName, child);
                if (agg != null) return agg;
            }
            DataFunction[] children = new DataFunction[this.operands.size()];
            for (int i = 0; i < operands.size(); ++i) {
                children[i] = operands.get(i);
            }
            DataFunction reg = DataFunction.customFunction(functionName, children);
            if (reg != null) return reg;
            throw new UnsupportedOperationException("Unknown function `" + functionName + "`" + " (with " + this.operands.size() + " argument(s))");
        }
    }

    public String getName(ASTExpression expression) {
        ExpressionNameBuilder builder = new ExpressionNameBuilder();
        expression.childrenAccept(builder, null);
        return builder.curr.toString();
    }

    public class ExpressionNameBuilder extends RookieParserDefaultVisitor {
        public StringBuilder curr = new StringBuilder();

        @Override
        public void visit(ASTLiteral node, Object data) {
            curr.append(node.jjtGetValue());
            super.visit(node, data);
        }

        @Override
        public void visit(ASTColumnName node, Object data) {
            curr.append(node.jjtGetValue());
            super.visit(node, data);
        }

        @Override
        public void visit(ASTIdentifier node, Object data) {
            curr.append(node.jjtGetValue());
            super.visit(node, data);
        }

        @Override
        public void visit(ASTLogicalOperator node, Object data) {
            curr.append(node.jjtGetValue());
            super.visit(node, data);
        }

        @Override
        public void visit(ASTComparisonOperator node, Object data) {
            curr.append(node.jjtGetValue());
            super.visit(node, data);
        }

        @Override
        public void visit(ASTExpression node, Object data) {
            if (node.jjtGetNumChildren() == 1 && node.jjtGetChild(0) instanceof ASTExpression) {
                curr.append("(");
                super.visit(node, data);
                curr.append(")");
                return;
            }
            super.visit(node, data);
        }

        @Override
        public void visit(ASTFunctionCallExpression node, Object data) {
            node.jjtGetChild(0).jjtAccept(this, data);
            if (node.jjtGetValue() != null) {
                curr.append("(*)");
                return;
            }
            curr.append("(");
            for (int i = 1; i < node.jjtGetNumChildren(); ++i) {
                node.jjtGetChild(i).jjtAccept(this, data);
                if (i != node.jjtGetNumChildren() - 1) curr.append(",");
            }
            curr.append(")");
        }

        @Override
        public void visit(ASTAdditiveOperator node, Object data) {
            curr.append(node.jjtGetValue());
            super.visit(node, data);
        }

        @Override
        public void visit(ASTMultiplicativeOperator node, Object data) {
            curr.append(node.jjtGetValue());
            super.visit(node, data);
        }
    }
}
