package edu.berkeley.cs186.database.cli.visitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.*;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class SelectStatementVisitor extends StatementVisitor {
    List<String> selectColumns = new ArrayList<>();
    List<String> tableNames = new ArrayList<>();
    List<String> tableAliases = new ArrayList<>();
    List<String> joinedTableLeftCols = new ArrayList<>();
    List<String> joinedTableRightCols = new ArrayList<>();
    List<PredicateOperator> predicateOperators = new ArrayList<>();
    List<String> predicateColumns = new ArrayList<>();
    List<DataBox> predicateValues = new ArrayList<>();
    List<String> groupByColumns = new ArrayList<>();
    int limit = -1;
    int offset = 0;

    @Override
    public void execute(Transaction transaction) {
        QueryPlan query = getQueryPlan(transaction).get();
        Iterator<Record> records = query.execute();
        PrettyPrinter.printRecords(selectColumns, records);
    }

    @Override
    public Optional<QueryPlan> getQueryPlan(Transaction transaction) {
        QueryPlan query = transaction.query(tableNames.get(0), tableAliases.get(0));
        for(int i = 1; i < tableNames.size(); i++) {
            query.join(
                tableNames.get(i),
                tableAliases.get(i),
                joinedTableLeftCols.get(i-1),
                joinedTableRightCols.get(i-1)
            );
        }
        for(int i = 0; i < predicateColumns.size(); i++) {
            query.select(
                predicateColumns.get(i),
                predicateOperators.get(i),
                predicateValues.get(i)
            );
        }
        if(!(selectColumns.size() == 1 && selectColumns.get(0) == "*")) {
            query.project(selectColumns);
        } else if(selectColumns.get(0) == "*") {
            selectColumns = new ArrayList<>();
            for(String alias: tableAliases) {
                Schema s = transaction.getSchema(alias);
                for(String colName: s.getFieldNames()) {
                    selectColumns.add((tableAliases.size() > 1 ? alias + ".": "") + colName);
                }
            }
            query.project(selectColumns);
        }
        if (groupByColumns.size() > 0) {
            query.groupBy(groupByColumns);
        }
        query.limit(limit, offset);
        return Optional.of(query);
    }

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.groupByColumns.add((String) node.jjtGetValue());
    }

    @Override
    public void visit(ASTResultColumnName node, Object data) {
        selectColumns.add((String) node.jjtGetValue());
    }

    @Override
    public void visit(ASTJoinedTable node, Object data) {
        ASTColumnName[] names = (ASTColumnName[]) node.jjtGetValue();
        this.joinedTableLeftCols.add((String) names[0].jjtGetValue());
        this.joinedTableRightCols.add((String) names[1].jjtGetValue());
        node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public void visit(ASTAliasedTableName node, Object data) {
        String[] names = (String[]) node.jjtGetValue();
        this.tableNames.add(names[0]);
        if(names[1] != null) this.tableAliases.add(names[1]);
        else this.tableAliases.add(names[0]);
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
        predicateColumns.add(col);
        predicateOperators.add(op);
        predicateValues.add(val);
    }

    @Override
    public void visit(ASTLimit node, Object data) {
        this.limit = Integer.parseInt((String) node.jjtGetValue());
    }

    public void prettyPrint() {
        System.out.println("SELECT");
        for(String c: selectColumns) {
            System.out.println("   " + c);
        }
        System.out.println("FROM");
        for(int i = 0; i < tableNames.size(); i++) {
            String name = tableNames.get(i);
            String alias = tableAliases.get(i);
            System.out.print("   " + name);
            if (alias != null) {
                System.out.print(" AS " + alias);
            }
            if (i > 0) {
                System.out.print(" ON ");
                System.out.print(joinedTableLeftCols.get(i-1));
                System.out.print(" = ");
                System.out.print(joinedTableRightCols.get(i-1));
            }
            System.out.println();
        }
        if(predicateColumns.size() > 0) {
            System.out.println("WHERE");
            for(int i = 0; i < predicateColumns.size(); i++) {
                System.out.printf(
                    "   %s %s %s\n",
                    predicateColumns.get(i),
                    predicateOperators.get(i),
                    predicateValues.get(i)
                );
            }
        }
        if(groupByColumns.size() > 0) {
            System.out.println("GROUP BY " + String.join(", ", groupByColumns));
        }
        if(limit != -1) {
            System.out.println("LIMIT " + limit);
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.SELECT;
    }
}