
package org.apache.drill.exec.store.couch;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.couch.common.CouchCompareOP;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

public class CouchFilterBuilder extends
        AbstractExprVisitor<CouchScanSpec, Void, RuntimeException>
{
    static final Logger logger = LoggerFactory
            .getLogger(CouchFilterBuilder.class);
    final CouchGroupScan groupScan;
    final LogicalExpression le;
    private boolean allExpressionsConverted = true;

    public CouchFilterBuilder(CouchGroupScan groupScan,
                              LogicalExpression conditionExp) {
        this.groupScan = groupScan;
        this.le = conditionExp;
    }

    public CouchScanSpec parseTree() {
        CouchScanSpec parsedSpec = le.accept(this, null);
        logger.info("what the fuck {}", parsedSpec.getFilters());
        if (parsedSpec != null) {
            parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getScanSpec(),
                    parsedSpec);
        }
        return parsedSpec;
    }

    private CouchScanSpec mergeScanSpecs(String functionName,
                                         CouchScanSpec leftScanSpec, CouchScanSpec rightScanSpec) {
        JSONObject newFilter = new JSONObject();

        switch (functionName) {
            case "booleanAnd":
                if (leftScanSpec.getFilters() != null
                        && rightScanSpec.getFilters() != null) {
                    newFilter = CouchUtils.andFilterAtIndex(leftScanSpec.getFilters(),
                            rightScanSpec.getFilters());
                } else if (leftScanSpec.getFilters() != null) {
                    newFilter = leftScanSpec.getFilters();
                } else {
                    newFilter = rightScanSpec.getFilters();
                }
                logger.info("{},{}",leftScanSpec.getFilters(),rightScanSpec.getFilters());
                break;
            case "booleanOr":
                newFilter = CouchUtils.orFilterAtIndex(leftScanSpec.getFilters(),
                        rightScanSpec.getFilters());
        }
        logger.info("couch filters {}", newFilter.toString());
        return new CouchScanSpec(groupScan.getScanSpec().getDbName(), groupScan
                .getScanSpec().getTableName(), newFilter);
    }

    public boolean isAllExpressionsConverted() {
        return allExpressionsConverted;
    }

    @Override
    public CouchScanSpec visitUnknown(LogicalExpression e, Void value)
            throws RuntimeException {
        allExpressionsConverted = false;
        return null;
    }

    @Override
    public CouchScanSpec visitBooleanOperator(BooleanOperator op, Void value) {
        List<LogicalExpression> args = op.args;
        CouchScanSpec nodeScanSpec = null;
        String functionName = op.getName();
        for (int i = 0; i < args.size(); ++i) {
            switch (functionName) {
                case "booleanAnd":
                case "booleanOr":
                    if (nodeScanSpec == null) {
                        nodeScanSpec = args.get(i).accept(this, null);
                    } else {
                        CouchScanSpec scanSpec = args.get(i).accept(this, null);
                        if (scanSpec != null) {
                            nodeScanSpec = mergeScanSpecs(functionName, nodeScanSpec, scanSpec);
                        } else {
                            allExpressionsConverted = false;
                        }
                    }
                    break;
            }
        }
        return nodeScanSpec;
    }

    @Override
    public CouchScanSpec visitFunctionCall(FunctionCall call, Void value)
            throws RuntimeException {
        CouchScanSpec nodeScanSpec = null;
        String functionName = call.getName();
        ImmutableList<LogicalExpression> args = call.args;

        if (CouchCompareFunctionProcessor.isCompareFunction(functionName)) {
            CouchCompareFunctionProcessor processor = CouchCompareFunctionProcessor
                    .process(call);
            if (processor.isSuccess()) {
                try {
                    nodeScanSpec = createCouchScanSpec(processor.getFunctionName(),
                            processor.getPath(), processor.getValue());
                } catch (Exception e) {
                    logger.error(" Failed to creare Filter ", e);
                    // throw new RuntimeException(e.getMessage(), e);
                }
            }
        } else {
            switch (functionName) {
                case "booleanAnd":
                case "booleanOr":
                    CouchScanSpec leftScanSpec = args.get(0).accept(this, null);
                    CouchScanSpec rightScanSpec = args.get(1).accept(this, null);
                    if (leftScanSpec != null && rightScanSpec != null) {
                        nodeScanSpec = mergeScanSpecs(functionName, leftScanSpec,
                                rightScanSpec);
                    } else {
                        allExpressionsConverted = false;
                        if ("booleanAnd".equals(functionName)) {
                            nodeScanSpec = leftScanSpec == null ? rightScanSpec : leftScanSpec;
                        }
                    }
                    break;
            }
        }

        if (nodeScanSpec == null) {
            allExpressionsConverted = false;
        }

        return nodeScanSpec;
    }

    private CouchScanSpec createCouchScanSpec(String functionName,
                                              SchemaPath field, Object fieldValue) throws ClassNotFoundException,
            IOException {
        // extract the field name
        String fieldName = field.getRootSegmentPath();
        logger.info("fieldname: {}", fieldName);
        CouchCompareOP compareOp = null;
        switch (functionName) {
            case "equal":
                compareOp = CouchCompareOP.EQUAL;
                break;
            case "not_equal":
                compareOp = CouchCompareOP.NOT_EQUAL;
                break;
            case "greater_than_or_equal_to":
                compareOp = CouchCompareOP.GREATER_OR_EQUAL;
                break;
            case "greater_than":
                compareOp = CouchCompareOP.GREATER;
                break;
            case "less_than_or_equal_to":
                compareOp = CouchCompareOP.LESS_OR_EQUAL;
                break;
            case "less_than":
                compareOp = CouchCompareOP.LESS;
                break;
            case "isnull":
            case "isNull":
            case "is null":
                compareOp = CouchCompareOP.IFNULL;
                break;
            case "isnotnull":
            case "isNotNull":
            case "is not null":
                compareOp = CouchCompareOP.IFNOTNULL;
                break;
        }

        if (compareOp != null) {
            JSONObject queryFilter = new JSONObject();
            if (compareOp == CouchCompareOP.IFNULL) {
                queryFilter.put(fieldName,
                        new JSONObject().put(CouchCompareOP.EQUAL.getCompareOp(),null));
            } else if (compareOp == CouchCompareOP.IFNOTNULL) {
                queryFilter.put(fieldName,
                        new JSONObject().put(CouchCompareOP.NOT_EQUAL.getCompareOp(),null));
            } else {
                JSONObject kv = new JSONObject();
                kv.put(compareOp.getCompareOp(),fieldValue);
                queryFilter.put(fieldName, kv);
                logger.info("go to else");
            }
            logger.info("filter builder {}, compareOp {}, filed value {}", queryFilter, compareOp.getCompareOp(), fieldValue);
            return new CouchScanSpec(groupScan.getScanSpec().getDbName(), groupScan
                    .getScanSpec().getTableName(), queryFilter);
        }
        return null;
    }
}
