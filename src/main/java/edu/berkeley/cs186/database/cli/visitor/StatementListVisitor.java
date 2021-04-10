package edu.berkeley.cs186.database.cli.visitor;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.cli.parser.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StatementListVisitor extends RookieParserDefaultVisitor {
    private Database database;
    public List<StatementVisitor> statementVisitors;

    public StatementListVisitor(Database database) {
        this.database = database;
        this.statementVisitors = new ArrayList<>();
    }

    public Transaction execute(Transaction currTransaction) {
        for (StatementVisitor visitor: this.statementVisitors) {
            switch(visitor.getType()) {
                case BEGIN:
                    if (currTransaction != null) {
                        System.out.println("WARNING: Transaction already in progress");
                    } else {
                        currTransaction = this.database.beginTransaction();
                    }
                    System.out.println("BEGIN");
                    break;
                case COMMIT:
                    if (currTransaction == null) {
                        System.out.println("WARNING: No transaction in progress");
                    } else {
                        currTransaction.commit();
                        currTransaction.close();
                        currTransaction = null;
                    }
                    System.out.println("COMMIT");
                    break;
                case ROLLBACK:
                    if (currTransaction == null) {
                        System.out.println("WARNING: No transaction in progress");
                    } else {
                        Optional<String> savepointName = visitor.getSavepointName();
                        if(savepointName.isPresent()) {
                            try {
                                currTransaction.rollbackToSavepoint(savepointName.get());
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.out.println("ROLLBACK TO SAVEPOINT failed.");
                                continue;
                            }
                        } else {
                            currTransaction.rollback();
                            currTransaction = null;
                        }
                    }
                    System.out.println("ROLLBACK");
                    break;
                default:
                    if (currTransaction == null) {
                        try (Transaction tmp = database.beginTransaction()) {
                            visitor.execute(tmp);
                            tmp.commit();
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("Operation failed.");
                        }
                    } else {
                        try {
                            visitor.execute(currTransaction);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("Operation failed.");
                        }
                    }
                break;
            }
        }
        return currTransaction;
    }

    /**
     * ROLLBACK
     */
    @Override
    public void visit(ASTRollbackStatement node, Object data) {
        RollbackStatementVisitor visitor = new RollbackStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * SAVEPOINT
     */
    @Override
    public void visit(ASTSavepointStatement node, Object data) {
        SavepointStatementVisitor visitor = new SavepointStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * RELEASE SAVEPOINT
     */
    @Override
    public void visit(ASTReleaseStatement node, Object data) {
        ReleaseStatementVisitor visitor = new ReleaseStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * COMMIT
     */
    @Override
    public void visit(ASTCommitStatement node, Object data) {
        CommitStatementVisitor visitor = new CommitStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * BEGIN
     */
    @Override
    public void visit(ASTBeginStatement node, Object data) {
        BeginStatementVisitor visitor = new BeginStatementVisitor();
        this.statementVisitors.add(visitor);
    }

    /**
     * SELECT
     */
    @Override
    public void visit(ASTSelectStatement node, Object data) {
        try {
            SelectStatementVisitor visitor = new SelectStatementVisitor();
            node.childrenAccept(visitor, null);
            this.statementVisitors.add(visitor);
        } catch (UnsupportedOperationException e) {
            System.out.println("Failed to execute SELECT");
            System.out.println(e.getMessage());
        }
    }

    /**
     * CREATE TABLE
     */
    @Override
    public void visit(ASTCreateTableStatement node, Object data) {
        CreateTableStatementVisitor visitor = new CreateTableStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * DROP TABLE
     */
    @Override
    public void visit(ASTDropTableStatement node, Object data) {
        DropTableStatementVisitor visitor = new DropTableStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * CREATE INDEX
     */
    @Override
    public void visit(ASTCreateIndexStatement node, Object data) {
        CreateIndexStatementVisitor visitor = new CreateIndexStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * DROP INDEX
     */
    @Override
    public void visit(ASTDropIndexStatement node, Object data) {
        DropIndexStatementVisitor visitor = new DropIndexStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * INSERT
     */
    @Override
    public void visit(ASTInsertStatement node, Object data) {
        InsertStatementVisitor visitor = new InsertStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * DELETE
     */
    @Override
    public void visit(ASTDeleteStatement node, Object data) {
        DeleteStatementVisitor visitor = new DeleteStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * UPDATE
     */
    @Override
    public void visit(ASTUpdateStatement node, Object data) {
        UpdateStatementVisitor visitor = new UpdateStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }

    /**
     * EXPLAIN
     */
    @Override
    public void visit(ASTExplainStatement node, Object data) {
        ExplainStatementVisitor visitor = new ExplainStatementVisitor();
        node.childrenAccept(visitor, null);
        this.statementVisitors.add(visitor);
    }
}
