package org.apache.flink.table.runtime.util;


import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.Row;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.Serializable;

public class RowDataStringSerializer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final InternalTypeInfo<RowData> type;

    public RowDataStringSerializer(InternalTypeInfo<RowData> type) {
        this.type = type;
    }

    public String asString(RowData row) {
        RowType rowType = type.toRowType();
        LogicalType[] fieldTypes =
        rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        int rowArity = rowType.getFieldCount();
        String rowString = "";
        for (int i = 0; i < rowArity; i++) {
            String value = "";
            if (row.isNullAt(i)) {
                value = "<NULL>";
            } else {
                switch (fieldTypes[i].getTypeRoot()) {
                        case NULL:
                            value = "<NULL>";
                            break;

                        case BOOLEAN:
                            value = row.getBoolean(i) ? "True" : "False";
                            break;

                        case INTEGER:
                        case INTERVAL_YEAR_MONTH:
                            value = Integer.toString(row.getInt(i));
                            break;

                        case BIGINT:
                        case INTERVAL_DAY_TIME:
                            value = Long.toString(row.getLong(i));
                            break;

                        case CHAR:
                        case VARCHAR:
                            value = row.getString(i).toString();
                            break;

                        case DATE:
                            value = "[DATE TYPE]";
                            break;

                        default:
                            value = "[Unprocessed type]";
                            break;
                }
            }
            String field = fieldNames[i] + "=" + value;
            rowString = rowString + field + (i == rowArity - 1 ? "" : ", ");
        }
        return rowString;
    }
}
