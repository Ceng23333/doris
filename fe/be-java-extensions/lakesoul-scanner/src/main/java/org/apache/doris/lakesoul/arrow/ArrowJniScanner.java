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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ScanPredicate;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class ArrowJniScanner extends JniScanner {

    private static final Logger LOG = Logger.getLogger(ArrowJniScanner.class);
    private final BufferAllocator allocator;

    public ArrowJniScanner(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    public ArrowJniScanner(BufferAllocator allocator, VectorSchemaRoot batch) {
        this(allocator);
        batchSize = batch.getRowCount();
        initTableInfo(batch.getSchema(), batchSize);
        vectorTable = loadVectorSchemaRoot(batch);
    }

    public VectorTable loadVectorSchemaRoot(VectorSchemaRoot batch) {
        Schema schema = batch.getSchema();
        int batchSize = batch.getRowCount();
        List<Field> fields = schema.getFields();

        ColumnType[] columnTypes = new ColumnType[fields.size()];
        String[] requiredFields = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            columnTypes[i] = ColumnType.parseType(fields.get(i).getName(), ArrowUtils.hiveTypeFromArrowField(fields.get(i)));
            requiredFields[i] = fields.get(i).getName();
        }
        BigIntVector metaAddressVector = new BigIntVector("metaAddress", allocator);

        int metaSize = 1;
        for (ColumnType type:columnTypes) {
            metaSize += type.metaSize();
        }
        metaAddressVector.allocateNew(metaSize);
        Integer idx = 0;

        // batchSize
        metaAddressVector.set(idx++, batchSize);

        for (int i = 0; i < requiredFields.length; i++) {
            ColumnType columnType = columnTypes[i];
            idx = fillMetaAddressVector(batchSize, columnType, metaAddressVector, idx, batch.getVector(i));
        }

        return VectorTable.createReadableTable(columnTypes, requiredFields, metaAddressVector.getDataBufferAddress());

    }

    protected void initTableInfo(Schema schema, int batchSize) {
        List<Field> fields = schema.getFields();

        ColumnType[] columnTypes = new ColumnType[fields.size()];
        String[] requiredFields = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            columnTypes[i] = ColumnType.parseType(fields.get(i).getName(), ArrowUtils.hiveTypeFromArrowField(fields.get(i)));
            requiredFields[i] = fields.get(i).getName();
        }
        ScanPredicate[] predicates = new ScanPredicate[0];

        super.initTableInfo(columnTypes, requiredFields, predicates, batchSize);
    }

    private Integer fillMetaAddressVector(int batchSize, ColumnType columnType, BigIntVector metaAddressVector, Integer offset, ValueVector valueVector) {
        // nullMap
        if (valueVector.getField().isNullable()) {
            metaAddressVector.set(offset++, ArrowUtils.loadValidityBuffer(valueVector.getValidityBuffer(), batchSize, allocator).memoryAddress());
        } else {
            metaAddressVector.set(offset++, 0);
        }

        if (columnType.isComplexType()) {
            if (!columnType.isStruct()) {
                // set offset buffer
                ArrowBuf offsetBuf = valueVector.getOffsetBuffer();
                metaAddressVector.set(offset++, ArrowUtils.loadOffsetBuffer(offsetBuf, batchSize, allocator, true).memoryAddress());
            }

            // set data buffer
            List<ColumnType> children = columnType.getChildTypes();
            for (int i = 0; i < children.size(); ++i) {

                ValueVector childrenVector = null;
                if (valueVector instanceof ListVector) {
                    childrenVector = ((ListVector) valueVector).getDataVector();
                } else if (valueVector instanceof StructVector) {
                    childrenVector = ((StructVector) valueVector).getVectorById(i);
                }
                offset = fillMetaAddressVector(batchSize, columnType.getChildTypes().get(i), metaAddressVector, offset, childrenVector);
            }

        } else if (columnType.isStringType()) {
            // set offset buffer
            ArrowBuf offsetBuf = valueVector.getOffsetBuffer();
            metaAddressVector.set(offset++,  ArrowUtils.loadOffsetBuffer(offsetBuf, batchSize, allocator, false).memoryAddress());

            // set data buffer
            metaAddressVector.set(offset++,  ((VarCharVector) valueVector).getDataBufferAddress());

        } else {
            // set data buffer
            metaAddressVector.set(offset++,  ((FieldVector) valueVector).getDataBufferAddress());
        }
        return offset;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        System.out.println(vectorTable.dump(batchSize));
    }

    @Override
    public int getNext() throws IOException {
        return 0;
    }
}
