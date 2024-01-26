package org.apache.doris.lakesoul.arrow;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.thrift.TPrimitiveType;

public class ArrowUtils {

    public static ColumnType columnTypeFromArrowField(Field field) {
        String hiveType = field.getType().accept(ArrowTypeToHiveTypeConverter.INSTANCE);
        return ColumnType.parseType(field.getName(), hiveType);
    }

    private static class ArrowTypeToHiveTypeConverter
        implements ArrowType.ArrowTypeVisitor<String> {

        private static final ArrowTypeToHiveTypeConverter INSTANCE =
            new ArrowTypeToHiveTypeConverter();

        @Override
        public String visit(ArrowType.Null type) {
            return "unsupported";
        }

        @Override
        public String visit(ArrowType.Struct type) {
            return "struct";
        }

        @Override
        public String visit(ArrowType.List type) {
            return "array";
        }

        @Override
        public String visit(ArrowType.LargeList type) {
            return "array";
        }

        @Override
        public String visit(ArrowType.FixedSizeList type) {
            return "array";
        }

        @Override
        public String visit(ArrowType.Union type) {
            return "unsupported";
        }

        @Override
        public String visit(ArrowType.Map type) {
            return "map";
        }

        @Override
        public String visit(ArrowType.Int type) {
            int bitWidth = type.getBitWidth();
            if (bitWidth <= 8) return "tinyint";
            if (bitWidth <= 2 * 8) return "smallint";
            if (bitWidth <= 4 * 8) return "int";
            return "bigint";
        }

        @Override
        public String visit(ArrowType.FloatingPoint type) {
            switch (type.getPrecision()) {
                case HALF:
                case SINGLE:
                    return "float";
                case DOUBLE:
                    return "double";
            }
            return "double";
        }

        @Override
        public String visit(ArrowType.Utf8 type) {
            return "string";
        }

        @Override
        public String visit(ArrowType.LargeUtf8 type) {
            return "string";
        }

        @Override
        public String visit(ArrowType.Binary type) {
            return "binary";
        }

        @Override
        public String visit(ArrowType.LargeBinary type) {
            return "binary";
        }

        @Override
        public String visit(ArrowType.FixedSizeBinary type) {
            return "binary";
        }

        @Override
        public String visit(ArrowType.Bool type) {
            return "boolean";
        }

        @Override
        public String visit(ArrowType.Decimal type) {
            return "decimal";
        }

        @Override
        public String visit(ArrowType.Date type) {
            return "datev1";
        }

        @Override
        public String visit(ArrowType.Time type) {
            int precision = 0;
            switch (type.getUnit()) {
                case SECOND:
                    precision = 0;
                    break;
                case MILLISECOND:
                    precision = 3;
                    break;
                case MICROSECOND:
                    precision = 6;
                    break;
                case NANOSECOND:
                    precision = 9;
            }
            return "datetimev2";
        }

        @Override
        public String visit(ArrowType.Timestamp type) {
            int precision = 0;
            switch (type.getUnit()) {
                case SECOND:
                    precision = 0;
                    break;
                case MILLISECOND:
                    precision = 3;
                    break;
                case MICROSECOND:
                    precision = 6;
                    break;
                case NANOSECOND:
                    precision = 9;
            }
            return "timestamp";
        }

        @Override
        public String visit(ArrowType.Interval type) {
            return "unsupported";
        }

        @Override
        public String visit(ArrowType.Duration type) {
            return "unsupported";
        }
    }


    public static class ArrowTypeToTPrimitiveTypeConverter
        implements ArrowType.ArrowTypeVisitor<TPrimitiveType> {

        public static final ArrowTypeToTPrimitiveTypeConverter INSTANCE =
            new ArrowTypeToTPrimitiveTypeConverter();


        @Override
        public TPrimitiveType visit(ArrowType.Null type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Struct type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.List type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.LargeList type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.FixedSizeList type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Union type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Map type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Int type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.FloatingPoint type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Utf8 type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.LargeUtf8 type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Binary type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.LargeBinary type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.FixedSizeBinary type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Bool type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Decimal type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Date type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Time type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Timestamp type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Interval type) {
            return null;
        }

        @Override
        public TPrimitiveType visit(ArrowType.Duration type) {
            return null;
        }
    }
}
