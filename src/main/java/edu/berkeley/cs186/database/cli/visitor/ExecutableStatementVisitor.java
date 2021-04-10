package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.*;
import edu.berkeley.cs186.database.query.QueryPlan;

import java.util.Optional;

public class ExecutableStatementVisitor extends RookieParserDefaultVisitor {
    StatementVisitor visitor = null;

    public Optional<QueryPlan> execute(Transaction transaction) {
        Optional<QueryPlan> qp = visitor.getQueryPlan(transaction);
        if (qp.isPresent()) return qp;
        visitor.execute(transaction);
        return Optional.empty();
    }

    /**
     * ROLLBACK
     */
    @Override
    public void visit(ASTRollbackStatement node, Object data) {
        this.visitor = new RollbackStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * SAVEPOINT
     */
    @Override
    public void visit(ASTSavepointStatement node, Object data) {
        this.visitor = new SavepointStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * RELEASE SAVEPOINT
     */
    @Override
    public void visit(ASTReleaseStatement node, Object data) {
        this.visitor = new ReleaseStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * COMMIT
     */
    @Override
    public void visit(ASTCommitStatement node, Object data) {
        this.visitor = new CommitStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * SELECT
     */
    @Override
    public void visit(ASTSelectStatement node, Object data) {
        this.visitor = new SelectStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * CREATE TABLE
     */
    @Override
    public void visit(ASTCreateTableStatement node, Object data) {
        this.visitor = new CreateTableStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * DROP TABLE
     */
    @Override
    public void visit(ASTDropTableStatement node, Object data) {
        this.visitor = new DropTableStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * CREATE INDEX
     */
    @Override
    public void visit(ASTCreateIndexStatement node, Object data) {
        this.visitor = new CreateIndexStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * DROP INDEX
     */
    @Override
    public void visit(ASTDropIndexStatement node, Object data) {
        this.visitor = new DropIndexStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * INSERT
     */
    @Override
    public void visit(ASTInsertStatement node, Object data) {
        this.visitor = new InsertStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * DELETE
     */
    @Override
    public void visit(ASTDeleteStatement node, Object data) {
        this.visitor = new DeleteStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * UPDATE
     */
    @Override
    public void visit(ASTUpdateStatement node, Object data) {
        this.visitor = new UpdateStatementVisitor();
        node.childrenAccept(visitor, null);
    }

    /**
     * EXPLAIN
     */
    @Override
    public void visit(ASTExplainStatement node, Object data) {
        this.visitor = new ExplainStatementVisitor();
        node.childrenAccept(visitor, null);
    }
}
