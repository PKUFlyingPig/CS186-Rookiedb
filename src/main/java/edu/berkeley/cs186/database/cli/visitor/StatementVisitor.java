package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.RookieParserDefaultVisitor;
import edu.berkeley.cs186.database.query.QueryPlan;

import java.util.Optional;

public abstract class StatementVisitor extends RookieParserDefaultVisitor {
    public void execute(Transaction transaction) {
        throw new UnsupportedOperationException("Statement is not executable.");
    }

    public Optional<String> getSavepointName() {
        return Optional.empty();
    }

    public Optional<QueryPlan> getQueryPlan(Transaction transaction) {
        return Optional.empty();
    }

    public abstract StatementType getType();
}