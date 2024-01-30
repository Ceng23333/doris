package org.apache.doris.lakesoul.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.DataSizeRoundingUtil;

import java.util.List;

import static org.apache.arrow.util.Preconditions.checkArgument;

public class ArrowUtils {
    public static ArrowBuf loadValidityBuffer(final ArrowBuf sourceValidityBuffer,
                                              final int valueCount,
                                              final BufferAllocator allocator) {
        ArrowBuf newBuffer = allocator.buffer(DataSizeRoundingUtil.roundUpTo8Multiple(valueCount));
        for (int newIdx = 0, sourceIdx = 0; newIdx < valueCount; newIdx+=8, sourceIdx++) {
            byte sourceByte = (byte) ~sourceValidityBuffer.getByte(sourceIdx);
            for (int i = 0; i < 8; i++) {
                newBuffer.writeByte(sourceByte & 1);
                sourceByte >>= 1;
            }
        }
        return newBuffer;
    }

    public static ArrowBuf loadOffsetBuffer(final ArrowBuf sourceOffsetBuffer,
                                            final int valueCount,
                                            final BufferAllocator allocator,
                                            final boolean isComplexType) {
        int length = valueCount << 3 ;
        if (isComplexType) length <<= 1;
        ArrowBuf newBuffer = allocator.buffer(length);
        for (int sourceIdx = 1; sourceIdx <= valueCount; sourceIdx++) {

            int sourceInt = sourceOffsetBuffer.getInt((long) sourceIdx << 2);
            if (isComplexType) {
                newBuffer.writeLong(sourceInt);
            } else {
                newBuffer.writeInt(sourceInt);
            }
        }
        return newBuffer;
    }

    public static String hiveTypeFromArrowField(Field field) {
        StringBuilder hiveType = new StringBuilder(field.getType().accept(ArrowTypeToHiveTypeConverter.INSTANCE));
        List<Field> children = field.getChildren();
        switch (hiveType.toString()) {
            case "array":
                checkArgument(children.size() == 1,
                    "Lists have one child Field. Found: %s", children.isEmpty() ? "none" : children);
                hiveType.append("<").append(hiveTypeFromArrowField(children.get(0))).append(">");
                break;
            case "struct":
                hiveType.append("<");
                boolean first = true;
                for (Field child: children) {
                    if (!first) {
                        hiveType.append(",");
                    } else {
                        first = false;
                    }
                    hiveType.append(child.getName()).append(":").append(hiveTypeFromArrowField(child));
                }
                hiveType.append(">");
                break;
        }
        return hiveType.toString();
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


}
