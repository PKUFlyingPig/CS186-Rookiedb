package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.PrettyPrinter;
import edu.berkeley.cs186.database.cli.parser.*;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.query.aggr.DataFunction;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class SelectStatementVisitor extends StatementVisitor {
    List<String> selectColumns = new ArrayList<>();
    List<String> selectAliases = new ArrayList<>();
    List<DataFunction> selectFunctions = new ArrayList<>();
    List<String> tableNames = new ArrayList<>();
    List<String> tableAliases = new ArrayList<>();
    List<String> joinedTableLeftCols = new ArrayList<>();
    List<String> joinedTableRightCols = new ArrayList<>();
    List<PredicateOperator> predicateOperators = new ArrayList<>();
    List<String> predicateColumns = new ArrayList<>();
    List<DataBox> predicateValues = new ArrayList<>();
    List<String> groupByColumns = new ArrayList<>();
    List<Pair<String, String>> contextAliases = new ArrayList<>();
    List<CommonTableExpressionVisitor> withExpressions = new ArrayList<>();
    String orderColumnName;
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
        List<Pair<String, String>> currentAliases = new ArrayList<>(contextAliases);
        for (CommonTableExpressionVisitor visitor: this.withExpressions) {
            String tempTableName = visitor.createTable(transaction, currentAliases);
            currentAliases.add(new Pair<>(tempTableName, visitor.name));
        }

        QueryPlan query = transaction.query(tableNames.get(0), tableAliases.get(0));
        for (Pair<String, String> tempTableName: currentAliases) {
            query.addTempTableAlias(tempTableName.getFirst(), tempTableName.getSecond());
        }
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
        ArrayList<String> expandedColumns = new ArrayList<>();
        ArrayList<DataFunction> expandedFunctions = new ArrayList<>();
        ArrayList<String> expandedAliases = new ArrayList<>();
        for (int i = 0; i < selectColumns.size(); i++) {
            String name = selectColumns.get(i);
            if (!name.contains("*") || selectFunctions.get(i) != null) {
                expandedColumns.add(name);
                expandedFunctions.add(selectFunctions.get(i));
                expandedAliases.add(selectAliases.get(i));
            } else if (name.equals("*")) {
                for(String alias: tableAliases) {
                    Schema s = transaction.getSchema(alias);
                    for (String colName : s.getFieldNames()) {
                        String qualifiedName = (tableAliases.size() > 1 ? alias + "." : "") + colName;
                        expandedColumns.add(qualifiedName);
                        expandedFunctions.add(new DataFunction.ColumnDataSource(qualifiedName));
                        expandedAliases.add(null);
                    }
                }
            } else {
                String[] s = name.split("\\.", 2);
                s[0] = s[0].trim();
                boolean found = false;
                for(String alias: tableAliases) {
                    if (!alias.toLowerCase().trim().equals(s[0].toLowerCase()))
                        continue;
                    Schema schema = transaction.getSchema(s[0]);
                    found=true;
                    for (String colName : schema.getFieldNames()) {
                        String qualifiedName = s[0] + "." + colName;
                        expandedColumns.add(qualifiedName);
                        expandedFunctions.add(new DataFunction.ColumnDataSource(qualifiedName));
                        expandedAliases.add(null);
                    }
                }
                if (!found) throw new UnsupportedOperationException("Unknown table `" + s[0] + "`");
            }
        }
        for (int i = 0; i < expandedColumns.size(); i++) {
            if (expandedAliases.get(i) != null) expandedColumns.set(i, expandedAliases.get(i));
        }
        query.project(expandedColumns, expandedFunctions);
        selectColumns = expandedColumns;
        selectFunctions = expandedFunctions;
        selectAliases = expandedAliases;

        if (groupByColumns.size() > 0) {
            query.groupBy(groupByColumns);
        }
        if (orderColumnName != null) {
            query.sort(orderColumnName);
        }
        query.limit(limit, offset);
        for (CommonTableExpressionVisitor visitor: this.withExpressions) {
            visitor.populateTable(transaction);
        }
        return Optional.of(query);
    }

    public void setContext(List<Pair<String, String>> context) {
        this.contextAliases = context;
    }

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.groupByColumns.add((String) node.jjtGetValue());
    }

    @Override
    public void visit(ASTCommonTableExpression node, Object data) {
        CommonTableExpressionVisitor visitor = new CommonTableExpressionVisitor();
        node.jjtAccept(visitor, data);
        this.withExpressions.add(visitor);
    }

    @Override
    public void visit(ASTSelectColumn node, Object data) {
        if (node.jjtGetValue().toString().startsWith("<>")) super.defaultVisit(node, data);

        String s = (String) node.jjtGetValue();
        if (s == null) s = "";
        if (s.toUpperCase().contains(" AS ")) {
            int o = s.toUpperCase().indexOf(" AS ");
            this.selectAliases.add(s.substring(o + 4));
            s = s.substring(0, o);
        } else {
            this.selectAliases.add(null);
        }
        if (node.jjtGetValue().toString().startsWith("<>")) return;
        this.selectFunctions.add(null);
        this.selectColumns.add(s.trim());
    }

    @Override
    public void visit(ASTExpression node, Object data) {
        ExpressionVisitor visitor = new ExpressionVisitor();
        node.jjtAccept(visitor, data);
        this.selectFunctions.add(visitor.build());
        this.selectColumns.add(visitor.getName(node));
    }

    @Override
    public void visit(ASTJoinedTable node, Object data) {
        String[] names = (String[]) node.jjtGetValue();
        this.joinedTableLeftCols.add(names[0]);
        this.joinedTableRightCols.add(names[1]);
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
    public void visit(ASTColumnValueComparison node, Object data) {
        ColumnValueComparisonVisitor visitor = new ColumnValueComparisonVisitor();
        node.jjtAccept(visitor, node);
        predicateColumns.add(visitor.columnName);
        predicateOperators.add(visitor.op);
        predicateValues.add(visitor.value);
    }

    @Override
    public void visit(ASTLimitClause node, Object data) {
        this.limit = (int) node.jjtGetValue();
    }

    @Override
    public void visit(ASTOrderClause node, Object data) {
        this.orderColumnName = (String) node.jjtGetValue();
    }

    @Override
    public StatementType getType() {
        return StatementType.SELECT;
    }
}