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
import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ScanPredicate;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LakeSoulArrowJniScanner extends JniScanner {

    protected static final Logger LOG = Logger.getLogger(LakeSoulArrowJniScanner.class);

    protected BufferAllocator allocator;
    private long  metaAddress = 0;
    private ArrayList<Long> extraOffHeap;

    protected Schema requiredSchema;

    public LakeSoulArrowJniScanner() {
    }

    public LakeSoulArrowJniScanner(BufferAllocator allocator) {
        metaAddress = 0;
        extraOffHeap = new ArrayList<>();
        withAllocator(allocator);
    }

    // for test
    public LakeSoulArrowJniScanner(BufferAllocator allocator, VectorSchemaRoot batch) {
        this(allocator);
        batchSize = batch.getRowCount();
        initTableInfo(batch.getSchema(), batchSize);
        vectorTable = loadVectorSchemaRoot(batch);
    }

    public void withAllocator(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    public VectorTable loadVectorSchemaRoot(VectorSchemaRoot batch) {
        int batchSize = batch.getRowCount();

        int metaSize = 1;
        for (ColumnType type : types) {
            metaSize += type.metaSize();
        }
        metaAddress = OffHeap.allocateMemory((long) metaSize << 3);
        OffHeap.putLong(null, metaAddress, batchSize);
        Integer idx = 1;

        for (int i = 0; i < fields.length; i++) {
            ColumnType columnType = types[i];
            idx = fillMetaAddressVector(batchSize, columnType, metaAddress, idx, batch.getVector(i));
        }

        return VectorTable.createReadableTable(types, fields, metaAddress);
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

    protected void initTableInfo(Map<String, String> params) {
        List<Field> fields = requiredSchema.getFields();

        ColumnType[] columnTypes = new ColumnType[fields.size()];
        String[] requiredFields = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            columnTypes[i] =
                ColumnType.parseType(fields.get(i).getName(),
                    ArrowUtils.hiveTypeFromArrowField(fields.get(i)));
            requiredFields[i] = fields.get(i).getName();
        }

        String predicatesAddressString = params.get("push_down_predicates");
        ScanPredicate[] predicates;
        if (predicatesAddressString == null) {
            predicates = new ScanPredicate[0];
        } else {
            long predicatesAddress = Long.parseLong(predicatesAddressString);
            if (predicatesAddress != 0) {
                predicates = ScanPredicate.parseScanPredicates(predicatesAddress, columnTypes);
                LOG.info("LakeSoulJniScanner gets pushed-down predicates:  " + ScanPredicate.dump(predicates));
            } else {
                predicates = new ScanPredicate[0];
            }
        }

        super.initTableInfo(columnTypes, requiredFields, predicates, batchSize);
    }

    private Integer fillMetaAddressVector(int batchSize, ColumnType columnType, long metaAddress, Integer offset,
                                          ValueVector valueVector) {
        // nullMap
        long validityBuffer = ArrowUtils.loadValidityBuffer(valueVector.getValidityBuffer(), batchSize, valueVector.getField().isNullable());
        extraOffHeap.add(validityBuffer);
        OffHeap.putLong(null, metaAddress + (offset++) * 8, validityBuffer);


        if (columnType.isComplexType()) {
            if (!columnType.isStruct()) {
                // set offset buffer
                ArrowBuf offsetBuf = valueVector.getOffsetBuffer();
                long offsetBuffer = ArrowUtils.loadComplexTypeOffsetBuffer(offsetBuf, batchSize);
                extraOffHeap.add(offsetBuffer);
                OffHeap.putLong(null, metaAddress + (offset++) * 8, offsetBuffer);
            }

            // set data buffer
            List<ColumnType> children = columnType.getChildTypes();
            for (int i = 0; i < children.size(); ++i) {

                ValueVector childrenVector;
                if (valueVector instanceof ListVector) {
                    childrenVector = ((ListVector) valueVector).getDataVector();
                } else if (valueVector instanceof StructVector) {
                    childrenVector = ((StructVector) valueVector).getVectorById(i);
                } else {
                    continue;
                }
                offset = fillMetaAddressVector(batchSize, columnType.getChildTypes().get(i), metaAddress, offset,
                    childrenVector);
            }

        } else if (columnType.isStringType()) {
            // set offset buffer
            ArrowBuf offsetBuf = valueVector.getOffsetBuffer();
            OffHeap.putLong(null, metaAddress + (offset++) * 8, offsetBuf.memoryAddress() + 4);

            // set data buffer
            OffHeap.putLong(null, metaAddress + (offset++) * 8, ((VarCharVector) valueVector).getDataBufferAddress());

        } else {
            // set data buffer
            OffHeap.putLong(null, metaAddress + (offset++) * 8, ((FieldVector) valueVector).getDataBufferAddress());
        }
        return offset;
    }

    public String dump() {
        return vectorTable.dump(batchSize);
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public int getNext() throws IOException {
        return 0;
    }

    @Override
    public void releaseTable() {
        if (metaAddress != 0) {
            OffHeap.freeMemory(metaAddress);
            metaAddress = 0;
        }
        for (long address:extraOffHeap) {
            OffHeap.freeMemory(address);
        }
        extraOffHeap.clear();
        vectorTable = null;
    }
}
