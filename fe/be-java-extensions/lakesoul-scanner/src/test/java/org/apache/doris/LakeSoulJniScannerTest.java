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

package org.apache.doris;

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.lakesoul.LakeSoulJniScanner;
import org.apache.doris.lakesoul.LakeSoulUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class LakeSoulJniScannerTest {
    @Test
    public void testLakeSoulJniScanner() throws IOException {
        OffHeap.setTesting();
        HashMap<String, String> params = new HashMap<>();
        params.put(LakeSoulUtils.FILE_NAMES, String.join(LakeSoulUtils.LIST_DELIM, Arrays.asList(
            "/Users/ceng/Desktop/lakesoul_table/range=range/part-00000-1c90ae74-e3a4-42b6-8f96-d4bbae715afb_00000.c000.parquet",
            "/Users/ceng/Desktop/lakesoul_table/range=range/part-00000-50a130a4-332a-4688-9929-38347d507ef8_00000.c000.parquet",
            "/Users/ceng/Desktop/lakesoul_table/range=range/part-00000-81f69370-6318-4403-bd3b-ffebb651298e_00000.c000.parquet"
        )));
        params.put(
            LakeSoulUtils.PRIMARY_KEYS,
            String.join(LakeSoulUtils.LIST_DELIM, Arrays.asList("hash1", "hash2"))
        );

        String schema = " {                      " +
            "   \"fields\" : [ {       " +
            "     \"name\" : \"range\",  " +
            "     \"nullable\" : true, " +
            "     \"type\" : {         " +
            "       \"name\" : \"utf8\"  " +
            "     },                 " +
            "     \"children\" : [ ]   " +
            "   }, {                 " +
            "     \"name\" : \"v1\",     " +
            "     \"nullable\" : true, " +
            "     \"type\" : {         " +
            "       \"name\" : \"utf8\"  " +
            "     },                 " +
            "     \"children\" : [ ]   " +
            "   }, {                 " +
            "     \"name\" : \"hash1\",  " +
            "     \"nullable\" : false," +
            "     \"type\" : {         " +
            "       \"name\" : \"int\",  " +
            "       \"bitWidth\" : 32, " +
            "       \"isSigned\" : true" +
            "     },                 " +
            "     \"children\" : [ ]   " +
            "   }, {                 " +
            "     \"name\" : \"v2\",     " +
            "     \"nullable\" : true, " +
            "     \"type\" : {         " +
            "       \"name\" : \"utf8\"  " +
            "     },                 " +
            "     \"children\" : [ ]   " +
            "   }, {                 " +
            "     \"name\" : \"hash2\",  " +
            "     \"nullable\" : false," +
            "     \"type\" : {         " +
            "       \"name\" : \"utf8\"  " +
            "     },                 " +
            "     \"children\" : [ ]   " +
            "   } ]                  " +
            " }";
        params.put(LakeSoulUtils.SCHEMA_JSON, schema);
        params.put(LakeSoulUtils.PARTITION_DESC, "range=range");

        LakeSoulJniScanner scanner = new LakeSoulJniScanner(1024, params);
        scanner.open();
        long metaAddress = 0;
        do {
            metaAddress = scanner.getNextBatchMeta();
            if (metaAddress != 0) {
                long rows = OffHeap.getLong(null, metaAddress);

                VectorTable restoreTable = VectorTable.createReadableTable(scanner.getTable().getColumnTypes(),
                    scanner.getTable().getFields(), metaAddress);
                System.out.println(restoreTable.dump((int) rows));
                // Restored table is release by the origin table.
            }

        } while (metaAddress != 0);
        scanner.close();
    }
}
