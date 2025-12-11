package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.table.Record;

import java.util.List;

public class MaterializeOperator extends SequentialScanOperator {
    /**
     * 将源操作符立即物化到临时表中的操作符，
     * 然后作为临时表上的顺序扫描操作符。
     * @param source 要物化的源操作符
     * @param transaction 当前运行的事务
     */
    public MaterializeOperator(QueryOperator source,
                        TransactionContext transaction) {
        super(OperatorType.MATERIALIZE, transaction, materializeToTable(source, transaction));
        setSource(source);
        setOutputSchema(source.getSchema());
    }

    private static String materializeToTable(QueryOperator source, TransactionContext transaction) {
        String materializedTableName = transaction.createTempTable(source.getSchema());
        for (Record record : source) {
            transaction.addRecord(materializedTableName, record);
        }
        return materializedTableName;
    }

    @Override
    public String str() {
        return "物化 (成本: " + this.estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return getSource().sortedBy();
    }
}
