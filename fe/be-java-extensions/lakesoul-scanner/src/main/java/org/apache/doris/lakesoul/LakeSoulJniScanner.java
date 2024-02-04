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

package org.apache.doris.lakesoul;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.google.common.base.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.lakesoul.arrow.ArrowJniScanner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.doris.lakesoul.LakeSoulUtils.*;

public class LakeSoulJniScanner extends ArrowJniScanner {

    private final Map<String, String> params;
    private NativeIOReader nativeIOReader;

    private transient LakeSoulArrowReader lakesoulArrowReader;

    private VectorSchemaRoot currentBatch = null;

    private final int awaitTimeout;

    public LakeSoulJniScanner(Map<String, String> params) {
        this.params = params;
        awaitTimeout = 10000;
    }

    @Override
    public void open() throws IOException {
        nativeIOReader = new NativeIOReader();
        withAllocator(nativeIOReader.getAllocator());

        // add files
        for (String file: params.get(FILE_NAMES).split(FILE_LIST_DELIM)) {
            nativeIOReader.addFile(file);
        }

        // set primary keys
        String primaryKeys = params.getOrDefault(PRIMARY_KEYS, "");
        if (!primaryKeys.isEmpty()) {
            nativeIOReader.setPrimaryKeys(Arrays.stream(primaryKeys.split(PRIMARY_KEYS_DELIM)).collect(Collectors.toList()));
        }

        nativeIOReader.setSchema(Schema.fromJSON(params.get(SCHEMA_JSON)));

        for (String partitionKV:params.getOrDefault(PARTITION_DESC, "").split(PARTITIONS_DELIM)) {
            if (partitionKV.isEmpty()) break;
            String[] kv = partitionKV.split(PARTITIONS_KV_DELIM);
            Preconditions.checkArgument(kv.length == 2, "Invalid partition column = " + partitionKV);
            nativeIOReader.setDefaultColumnValue(kv[0], kv[1]);
        }
        nativeIOReader.initializeReader();
        lakesoulArrowReader = new LakeSoulArrowReader(nativeIOReader, awaitTimeout);
    }

    @Override
    public void close() throws IOException {
        super.close();
        releaseTable();
        if (lakesoulArrowReader != null) {
            lakesoulArrowReader.close();
        }
    }

    @Override
    public int getNext() throws IOException {
        if (lakesoulArrowReader.hasNext()) {
            currentBatch = lakesoulArrowReader.nextResultVectorSchemaRoot();
            batchSize = currentBatch.getRowCount();
            initTableInfo(currentBatch.getSchema(), batchSize);
            vectorTable = loadVectorSchemaRoot(currentBatch);
            return batchSize;
        } else {
            return 0;
        }
    }

    @Override
    public long getNextBatchMeta() throws IOException {
        int numRows;
        try {
            numRows = getNext();
        } catch (IOException e) {
            releaseTable();
            throw e;
        }
        if (numRows == 0) {
            releaseTable();
            return 0;
        }
        assert (numRows == vectorTable.getNumRows());
        return vectorTable.getMetaAddress();
    }

    @Override
    public void releaseTable() {
        super.releaseTable();
        if (currentBatch != null) {
            currentBatch.close();
        }
    }
}
