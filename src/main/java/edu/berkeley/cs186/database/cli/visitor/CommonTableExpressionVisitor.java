package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.cli.parser.ASTIdentifier;
import edu.berkeley.cs186.database.cli.parser.ASTSelectStatement;
import edu.berkeley.cs186.database.cli.parser.RookieParserDefaultVisitor;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CommonTableExpressionVisitor extends RookieParserDefaultVisitor {
    String name = null;
    String alias = null;
    List<String> columns = new ArrayList<>();
    ASTSelectStatement child = null;
    SelectStatementVisitor visitor = null;

    public String createTable(Transaction transaction, List<Pair<String, String>> outerContext) {
        this.visitor = new SelectStatementVisitor();
        this.visitor.setContext(new ArrayList<>(outerContext));
        this.child.jjtAccept(this.visitor, null);
        QueryPlan p = this.visitor.getQueryPlan(transaction).get();
        p.execute();
        Schema schema = p.getFinalOperator().getSchema();
        if (this.columns.size() != 0) {
            if (schema.size() != this.columns.size()) {
                throw new UnsupportedOperationException("Number of columns in WITH statement doesn't match number of columns in subquery.");
            }
            Schema prev = schema;
            schema = new Schema();
            for (int i = 0; i < prev.size(); i++) {
                schema.add(columns.get(i), prev.getFieldType(i));
            }
        }
        TransactionContext tc = transaction.getTransactionContext();
        TransactionContext.setTransaction(tc);
        try {
            this.alias = tc.createTempTable(schema);
        } finally {
            TransactionContext.unsetTransaction();
        }
        return this.alias;
    }

    public void populateTable(Transaction transaction) {
        Iterator<Record> records = this.visitor.getQueryPlan(transaction).get().execute();
        while (records.hasNext()) {
            transaction.insert(this.alias, records.next());
        }
    }

    @Override
    public void visit(ASTIdentifier node, Object data) {
        if (this.name == null) this.name = (String) node.jjtGetValue();
        else this.columns.add((String) node.jjtGetValue());
    }

    @Override
    public void visit (ASTSelectStatement node, Object data) {
        this.child = node;
    }
}
