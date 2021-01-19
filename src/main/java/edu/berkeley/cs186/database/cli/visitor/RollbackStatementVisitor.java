package edu.berkeley.cs186.database.cli.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.cli.parser.*;

public class RollbackStatementVisitor extends StatementVisitor {
    public String savepointName;
    public List<DataBox> values = new ArrayList<DataBox>();

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.savepointName = (String) node.jjtGetValue();
    }

    @Override
    public StatementType getType() {
        return StatementType.ROLLBACK;
    }

    @Override
    public Optional<String> getSavepointName() {
        if (savepointName != null) {
            return Optional.of(this.savepointName);
        }
        return Optional.empty();
    }
}