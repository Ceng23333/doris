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

package org.apache.doris.catalog.external;

import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalCatalog;
import org.apache.doris.thrift.TLakeSoulTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LakeSoulExternalTable extends ExternalTable {

    public static final int LAKESOUL_TIMESTAMP_SCALE_MS = 6;

    public LakeSoulExternalTable(long id, String name, String dbName, LakeSoulExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.LAKESOUl_EXTERNAL_TABLE);
    }

    private Type lakeSoulTypeToDorisType(ArrowType dt) {
        if (dt instanceof ArrowType.Bool) {
            return Type.BOOLEAN;
        } else if (dt instanceof ArrowType.Int) {
            ArrowType.Int type = (ArrowType.Int) dt;
            switch (type.getBitWidth()) {
                case 16:
                    return Type.SMALLINT;
                case 32:
                    return Type.INT;
                case 64:
                    return Type.BIGINT;
                default:
                    throw new IllegalArgumentException("Invalid integer bit width: " + type.getBitWidth() +
                        " for LakeSoul table: " + getTableIdentifier());
            }
        } else if (dt instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint type = (ArrowType.FloatingPoint) dt;
            switch (type.getPrecision()) {
                case SINGLE:
                    return Type.FLOAT;
                case DOUBLE:
                    return Type.DOUBLE;
                default:
                    throw new IllegalArgumentException("Invalid floating point precision: " + type.getPrecision() +
                        " for LakeSoul table: " + getTableIdentifier());
            }
        } else if (dt instanceof ArrowType.Utf8) {
            return Type.STRING;
        } else if (dt instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimalType = (ArrowType.Decimal) dt;
            return ScalarType.createDecimalV3Type(decimalType.getPrecision(), decimalType.getScale());
        } else if (dt instanceof ArrowType.Date) {
            return ScalarType.createDateV2Type();
        } else if (dt instanceof ArrowType.Timestamp) {
            return ScalarType.createDatetimeV2Type(LAKESOUL_TIMESTAMP_SCALE_MS);
        }
        throw new IllegalArgumentException("Cannot transform type " + dt + " to doris type" +
            " for LakeSoul table " + getTableIdentifier());
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TLakeSoulTable tLakeSoulTable = new TLakeSoulTable(dbName, name, new HashMap<>());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.LAKESOUL_TABLE, schema.size(), 0,
            getName(), dbName);
        tTableDescriptor.setLakesoulTable(tLakeSoulTable);
        return tTableDescriptor;

    }

    @Override
    public List<Column> initSchema() {
        String tableSchema = ((LakeSoulExternalCatalog) catalog).getLakeSoulTable(dbName, name).getTableSchema();
        System.out.println(tableSchema);
        Schema schema;
        try {
            schema = Schema.fromJSON(tableSchema);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<Column> tmpSchema = Lists.newArrayListWithCapacity(schema.getFields().size());
        for (Field field : schema.getFields()) {
            tmpSchema.add(new Column(new Column(field.getName(), lakeSoulTypeToDorisType(field.getType()),
                true, null, true,
                    field.getMetadata().getOrDefault("comment", null),
                true, schema.getFields().indexOf(field))));
        }
        return tmpSchema;
    }

    public TableInfo getLakeSoulTableInfo() {
        return ((LakeSoulExternalCatalog) catalog).getLakeSoulTable(dbName, name);
    }

    public String tablePath() {
        return ((LakeSoulExternalCatalog) catalog).getLakeSoulTable(dbName, name).getTablePath();
    }

    public Map<String, String> getHadoopProperties() {
        return catalog.getCatalogProperty().getHadoopProperties();
    }

    public String getTableIdentifier() {
        return dbName + "." + name;
    }
}

