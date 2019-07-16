
package org.apache.drill.exec.store.couch;

import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class CouchScanBatchCreator implements BatchCreator<CouchSubScan>{
    static final Logger logger = LoggerFactory
            .getLogger(CouchScanBatchCreator.class);

    @Override
    public ScanBatch getBatch(ExecutorFragmentContext context, CouchSubScan subScan,
                              List<RecordBatch> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        List<RecordReader> readers = new LinkedList<>();
        List<SchemaPath> columns = null;
        if ((columns = subScan.getColumns()) == null)
        {
            columns = GroupScan.ALL_COLUMNS;
            logger.info(columns +"columns");
        }
        CouchSubScan.CouchSubScanSpec subscanspec = subScan.getSubScanSpec();
        readers.add(new CouchRecordReader(context,columns,subscanspec,subScan.getCouchStoragePlugin()));
        logger.info(columns.toString() + "scanBatch columns");
        logger.info("Number of record readers initialized : " + readers.size());
        return new ScanBatch(subScan, context, readers);
    }
}
