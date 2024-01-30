package org.apache.doris;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.lakesoul.arrow.ArrowJniScanner;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class ArrowTest {

    final static String STRUCT_INT_CHILD = "struct_int_child";
    final static String STRUCT_UTF8_CHILD = "struct_utf8_child";

    @Test
    public void testReadVectorSchemaRoot() throws IOException {
        OffHeap.setTesting();
        BufferAllocator allocator = new RootAllocator();
        ArrowJniScanner scanner = new ArrowJniScanner(allocator, createVectorSchemaRoot(allocator));
        scanner.open();
//        long metaAddress = 0;
//        do {
//            metaAddress = scanner.getNextBatchMeta();
//            if (metaAddress != 0) {
//                long rows = OffHeap.getLong(null, metaAddress);
//                Assert.assertEquals(3, rows);
//
//                VectorTable restoreTable = VectorTable.createReadableTable(scanner.getTable().getColumnTypes(),
//                    scanner.getTable().getFields(), metaAddress);
//                System.out.println(restoreTable.dump((int) rows));
//                // Restored table is release by the origin table.
//            }
////            scanner.resetTable();
//        } while (metaAddress != 0);
//        scanner.releaseTable();
        scanner.close();
    }

    public static VectorSchemaRoot createVectorSchemaRoot(BufferAllocator allocator) {
        Schema schema = new Schema(
            Arrays.asList(
                new Field("int",  FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("utf8",  FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("list", FieldType.nullable(new ArrowType.List()),
                    Collections.singletonList(new Field("list_int", FieldType.nullable(new ArrowType.Int(32, true)), null))),
                new Field("struct", FieldType.nullable(new ArrowType.Struct()),
                    Arrays.asList(new Field(STRUCT_INT_CHILD, FieldType.nullable(new ArrowType.Int(32, true)), null)
                        ))
            )
        );
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        int batchSize = 16;
        root.setRowCount(batchSize);
        for (int idx=0; idx < schema.getFields().size(); idx ++) {
            setValue(root, root.getVector(idx), idx, batchSize);
        }

        System.out.println(root.contentToTSVString());
        return root;
    }

    private static void setValue(VectorSchemaRoot root, FieldVector fieldVector, int columnIdx, int batchSize) {
        if (fieldVector instanceof TinyIntVector) {
            TinyIntVector vector = (TinyIntVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3 );
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof SmallIntVector) {
            SmallIntVector vector = (SmallIntVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3 );
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof IntVector) {
            IntVector vector = (IntVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3 );
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof BigIntVector) {
            BigIntVector vector = (BigIntVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7L + i * 3L);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof BitVector) {
            BitVector vector = (BitVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof Float4Vector) {
            Float4Vector vector = (Float4Vector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof Float8Vector) {
            Float8Vector vector = (Float8Vector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof VarCharVector) {
            VarCharVector vector = (VarCharVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, new Text(String.valueOf(columnIdx * 101 + i * 3)));
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof FixedSizeBinaryVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        } else if (fieldVector instanceof VarBinaryVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        } else if (fieldVector instanceof DecimalVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));

        } else if (fieldVector instanceof DateDayVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        } else if (fieldVector instanceof TimeSecVector
            || fieldVector instanceof TimeMilliVector
            || fieldVector instanceof TimeMicroVector
            || fieldVector instanceof TimeNanoVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        } else if (fieldVector instanceof TimeStampVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));

        } else if (fieldVector instanceof MapVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));

        } else if (fieldVector instanceof ListVector) {
            ListVector vector = (ListVector) fieldVector;
            vector.allocateNew();
            UnionListWriter writer = vector.getWriter();
            int count = 0;
            for (int i = 0; i < batchSize; i++) {
                writer.startList();
                int subCount = (columnIdx * 7 + i * 3) % 5;
                writer.setPosition(i);
                for (int j = 0; j < subCount; j++) {
                    writer.writeInt(columnIdx * 7 + i * 3 + j * 11);
                }
                writer.setValueCount(subCount);
                count += subCount;

                writer.endList();
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }

            vector.setValueCount(count);
        } else if (fieldVector instanceof StructVector) {
            StructVector vector = (StructVector) fieldVector;
            NullableStructWriter writer = vector.getWriter();
            IntWriter intWriter = writer.integer(STRUCT_INT_CHILD);
//            VarCharWriter varCharWriter = writer.varChar(STRUCT_UTF8_CHILD);
            for (int i = 0; i < batchSize; i++) {
                writer.setPosition(i);
                writer.start();
                intWriter.writeInt(columnIdx * 7 + i * 3);
//                varCharWriter.writeVarChar(new Text(String.valueOf(columnIdx * 101 + i * 3)));
                writer.end();
            }

            writer.setValueCount(batchSize);
        } else if (fieldVector instanceof NullVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        } else {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        }
    }


}
