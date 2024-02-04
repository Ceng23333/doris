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
import org.apache.doris.catalog.*;
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

        if (dt instanceof ArrowType.Binary) {
            return Type.BOOLEAN;
        } else if (dt instanceof ArrowType.Int) {
            if(((ArrowType.Int)dt).getIsSigned() && ((ArrowType.Int)dt).getBitWidth()==32) {return Type.INT;}
        } else if (dt instanceof ArrowType.Int) {
            if(((ArrowType.Int)dt).getIsSigned() && ((ArrowType.Int)dt).getBitWidth()==16) {return Type.SMALLINT;}
        } else if (dt instanceof ArrowType.Int) {
            if(((ArrowType.Int)dt).getIsSigned() && ((ArrowType.Int)dt).getBitWidth()==64) {return Type.BIGINT;}
        } else if (dt instanceof ArrowType.FloatingPoint) {
            if(((ArrowType.FloatingPoint)dt).getPrecision()== FloatingPointPrecision.SINGLE) {return Type.FLOAT;}
        } else if (dt instanceof ArrowType.FloatingPoint) {
            if(((ArrowType.FloatingPoint)dt).getPrecision()== FloatingPointPrecision.DOUBLE) {return Type.DOUBLE;}
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
        throw new IllegalArgumentException("Cannot transform unknown type: " + dt);
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
                e.printStackTrace();
                throw new RuntimeException(e);
        }

        List<Column> tmpSchema = Lists.newArrayListWithCapacity(schema.getFields().size());
        for (Field field : schema.getFields()) {
            tmpSchema.add(new Column(new Column(field.getName(), lakeSoulTypeToDorisType(field.getType()),
                    true, null, true,
                    field.getMetadata().containsKey("comment") ? field.getMetadata().get("comment") : null,
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
}

