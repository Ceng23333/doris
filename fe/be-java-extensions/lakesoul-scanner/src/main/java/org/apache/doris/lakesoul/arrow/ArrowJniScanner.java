// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.lakesoul.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.common.jni.vec.ScanPredicate;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ArrowJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(ArrowJniScanner.class);
    private final VectorSchemaRoot batch;

    private int mockRows = 10;
    private int readRows = 0;

    public ArrowJniScanner(VectorSchemaRoot batch) {
        this.batch = batch;
        Schema schema = batch.getSchema();
        List<Field> fields = schema.getFields();

        ColumnType[] columnTypes = new ColumnType[fields.size()];
        String[] requiredFields = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            columnTypes[i] = ArrowUtils.columnTypeFromArrowField(fields.get(i));
            requiredFields[i] = fields.get(i).getName();
        }

        ScanPredicate[] predicates = new ScanPredicate[0];

        initTableInfo(columnTypes, requiredFields, predicates, batch.getRowCount());
    }

    @Override
    protected void initTableInfo(ColumnType[] requiredTypes, String[] requiredFields, ScanPredicate[] predicates,
                                 int batchSize) {
        super.initTableInfo(requiredTypes, requiredFields, predicates, batchSize);

    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public int getNext() throws IOException {
        if (readRows == mockRows) {
            return 0;
        }
        int batchSize = batch.getRowCount();
        int rows = Math.min(batchSize, mockRows - readRows);

        int numColumn = batch.getFieldVectors().size();
        for (int i = 0; i < numColumn; i++) {
            System.out.println(i);
            vectorTable.appendData(i, new Object[]{batch.getVector(i)}, new ArrowColumnValueConverter(rows), batch.getSchema().getFields().get(i).isNullable());
        }
        readRows += rows;
        return rows;
    }
}
