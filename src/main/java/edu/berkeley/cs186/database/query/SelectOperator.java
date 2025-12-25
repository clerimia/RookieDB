package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SelectOperator extends QueryOperator {
    private int columnIndex;
    private String columnName;
    private PredicateOperator operator;
    private DataBox value;

    /**
     * 创建一个新的SelectOperator，它从source中获取数据，
     * 并只返回满足谓词条件的元组。
     *
     * @param source 该操作符的数据源
     * @param columnName 需要评估谓词的列名
     * @param operator 实际的比较器
     * @param value 要比较的值
     */
    public SelectOperator(QueryOperator source,
                   String columnName,
                   PredicateOperator operator,
                   DataBox value) {
        super(OperatorType.SELECT, source);
        this.operator = operator;
        this.value = value;

        this.columnIndex = this.getSchema().findField(columnName);
        this.columnName = this.getSchema().getFieldName(columnIndex);

        this.stats = this.estimateStats();
    }

    @Override
    public boolean isSelect() {
        return true;
    }

    @Override
    public Schema computeSchema() {
        return this.getSource().getSchema();
    }

    @Override
    public String str() {
        return String.format("Select %s%s%s (Cost=%d)",
                this.columnName,
                this.operator.toSymbol(),
                this.value,
                this.estimateIOCost()
        );
    }

    /**
     * 估算执行此查询操作符的结果的表统计信息。
     *
     * @return 估算的TableStats
     */
    @Override
    public TableStats estimateStats() {
        TableStats stats = this.getSource().estimateStats();
        return stats.copyWithPredicate(this.columnIndex,
                                       this.operator,
                                       this.value);
    }

    @Override
    public int estimateIOCost() {
        return this.getSource().estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() { return new SelectIterator(); }

    /**
     * 为该操作符提供迭代器接口的Iterator实现。
     */
    private class SelectIterator implements Iterator<Record> {
        private Iterator<Record> sourceIterator;
        private Record nextRecord;

        private SelectIterator() {
            this.sourceIterator = SelectOperator.this.getSource().iterator();
            this.nextRecord = null;
        }

        /**
         * 检查是否有更多的记录可以产出
         * 这个函数会尝试从源迭代器中产生一个记录，也就是给nextRecord赋值
         * 如果已经缓存了一个就返回true
         * 如果没有缓存记录，就调用源迭代器的next知道获取一条满足要求记录返回true
         * 的或者没有记录了返回false
         * @return 如果此迭代器还有另一个可产出的记录则返回true，否则返回false
         */
        @Override
        public boolean hasNext() {
            // 如果当前还缓存有记录，直接返回True
            if (this.nextRecord != null) {
                return true;
            }
            // 如果没有缓存记录了，就尝试向源操作符迭代器获取记录
            while (this.sourceIterator.hasNext()) {
                Record r = this.sourceIterator.next();
                switch (SelectOperator.this.operator) {
                case EQUALS:
                    if (r.getValue(SelectOperator.this.columnIndex).equals(value)) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                case NOT_EQUALS:
                    if (!r.getValue(SelectOperator.this.columnIndex).equals(value)) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                case LESS_THAN:
                    if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) < 0) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                case LESS_THAN_EQUALS:
                    if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) < 0) {
                        this.nextRecord = r;
                        return true;
                    } else if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) == 0) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                case GREATER_THAN:
                    if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) > 0) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                case GREATER_THAN_EQUALS:
                    if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) > 0) {
                        this.nextRecord = r;
                        return true;
                    } else if (r.getValue(SelectOperator.this.columnIndex).compareTo(value) == 0) {
                        this.nextRecord = r;
                        return true;
                    }
                    break;
                default:
                    break;
                }
            }
            return false;
        }

        /**
         * 产生此迭代器的下一个记录。
         *
         * @return 下一个Record
         * @throws NoSuchElementException 如果没有更多记录可产生
         */
        @Override
        public Record next() {
            if (this.hasNext()) {
                Record r = this.nextRecord;
                this.nextRecord = null;
                return r;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            // 没有提供的操作
            throw new UnsupportedOperationException();
        }
    }
}
