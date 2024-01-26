package org.apache.doris;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.lakesoul.arrow.ArrowJniScanner;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ArrowTest {

    private static BufferAllocator allocator = new RootAllocator();

    public static VectorSchemaRoot createVectorSchemaRoot() {
        Schema schema = new Schema(
            Arrays.asList(
                Field.nullable("age",  new ArrowType.Int(32, true)),
                Field.nullable("name",  new ArrowType.Utf8())
            )
        );
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntVector ageVector = (IntVector) root.getVector("age");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        root.setRowCount(3);
        ageVector.allocateNew(3);
        ageVector.set(0, 10);
        ageVector.set(1, 20);
        ageVector.set(2, 30);
        nameVector.allocateNew(3);
        nameVector.set(0, "Dave".getBytes(StandardCharsets.UTF_8));
        nameVector.set(1, "Peter".getBytes(StandardCharsets.UTF_8));
        nameVector.set(2, "Mary".getBytes(StandardCharsets.UTF_8));
        return root;
    }

    @Test
    public void testReadVectorSchemaRoot() throws IOException {
        OffHeap.setTesting();
        ArrowJniScanner scanner = new ArrowJniScanner(createVectorSchemaRoot());
        scanner.open();
        long metaAddress = 0;
        do {
            metaAddress = scanner.getNextBatchMeta();
            if (metaAddress != 0) {
                long rows = OffHeap.getLong(null, metaAddress);
//                Assert.assertEquals(32, rows);

                VectorTable restoreTable = VectorTable.createReadableTable(scanner.getTable().getColumnTypes(),
                    scanner.getTable().getFields(), metaAddress);
                System.out.println(restoreTable.dump((int) rows));
                // Restored table is release by the origin table.
            }
            scanner.resetTable();
        } while (metaAddress != 0);
        scanner.releaseTable();
        scanner.close();
    }
}
