package org.apache.doris.lakesoul.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.doris.common.jni.vec.ColumnValueConverter;

import java.nio.IntBuffer;
import java.util.Arrays;

public class ArrowColumnValueConverter implements ColumnValueConverter {

    private final int rows;

    public ArrowColumnValueConverter(int rows) {
        this.rows = rows;
    }

    @Override
    public Object[] convert(Object[] column) {
        assert column.length == 1;
        if (column[0] instanceof IntVector) {
            IntVector vector = (IntVector) column[0];

            Integer[] values = new Integer[rows];
            for (int i = 0; i < rows; i++) {
                values[i] = vector.get(i);
            }
            return values;
        }
        if (column[0] instanceof VarCharVector) {
            VarCharVector vector = (VarCharVector) column[0];
            String[] values = new String[rows];
            for (int i = 0; i < rows; i++) {
                values[i] = new String(vector.get(i));
            }
            return values;
        }
        return null;
    }
}
