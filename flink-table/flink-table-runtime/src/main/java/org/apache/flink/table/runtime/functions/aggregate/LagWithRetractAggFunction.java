package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.LinkedHashSetSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;


public class LagWithRetractAggFunction<T> extends BuiltInAggregateFunction<T, LagWithRetractAggFunction.LagWithRetractAccumulator<T>> {
    private final transient DataType[] valueDataTypes;

    public LagWithRetractAggFunction(LogicalType[] valueTypes) {
        this.valueDataTypes =
                Arrays.stream(valueTypes)
                        .map(DataTypeUtils::toInternalDataType)
                        .toArray(DataType[]::new);
        if (valueDataTypes.length == 3
                && valueDataTypes[2].getLogicalType().getTypeRoot() != LogicalTypeRoot.NULL) {
            if (valueDataTypes[0].getConversionClass() != valueDataTypes[2].getConversionClass()) {
                throw new TableException(
                        String.format(
                                "Please explicitly cast default value %s to %s.",
                                valueDataTypes[2], valueDataTypes[1]));
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Arrays.asList(valueDataTypes);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                LagWithRetractAccumulator.class,
                DataTypes.FIELD("offset", DataTypes.INT()),
                DataTypes.FIELD("defaultValue", valueDataTypes[0]),
                DataTypes.FIELD("buffer", getLinkedHashSetType()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private DataType getLinkedHashSetType() {
        TypeSerializer<T> serializer =
                InternalSerializers.create(getOutputDataType().getLogicalType());
        return DataTypes.RAW(
                LinkedHashSet.class, (TypeSerializer) new LinkedHashSetSerializer<>(serializer));
    }

    @Override
    public DataType getOutputDataType() {
        return valueDataTypes[0];
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    /** Accumulator for LAG. */
    public static class LagWithRetractAccumulator<T> {
        static int UNIQUE_ID = 0;
        int uid = ++UNIQUE_ID;
        public int offset = 1;
        public T defaultValue = null;
        public LinkedHashSet<T> buffer = new LinkedHashSet<>();

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LagWithRetractAccumulator<?> lagAcc = (LagWithRetractAccumulator<?>) o;
            return offset == lagAcc.offset
                    && Objects.equals(defaultValue, lagAcc.defaultValue)
                    && Objects.equals(buffer, lagAcc.buffer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, defaultValue, buffer);
        }

        public int uniqueID() {
            return uid;
        }
    }

    public void accumulate(LagWithRetractAccumulator<T> acc, T value) throws Exception {
        LOG.info(acc.uniqueID() + ": Accumulating value " + value.toString());
        acc.buffer.add(value);
        // We only keep 10x the offset needed in our state to keep memory bounded. This
        // can cause incorrect result. So, adding a warning to the accumulator.
        while (acc.buffer.size() > 10 * acc.offset) {
            for (T element : acc.buffer) {
               acc.buffer.remove(element);
               break;
            }
            LOG.info("Dropping value. Results will be incorrect.");
        }
        // TODO(akhilg): Add logging.
    }

    public void accumulate(LagWithRetractAccumulator<T> acc, T value, int offset) throws Exception {
        if (offset < 0) {
            throw new TableException(String.format("Offset(%d) should be positive.", offset));
        }

        acc.offset = offset;
        accumulate(acc, value);
    }

    public void retract(LagWithRetractAccumulator<T> acc, Object value) throws Exception {
        LOG.info(acc.uniqueID() + ": Removing value " + value.toString());
        acc.buffer.remove(value);
    }

    public void accumulate(LagWithRetractAccumulator<T> acc, T value, int offset, T defaultValue) throws Exception {
        acc.defaultValue = defaultValue;
        accumulate(acc, value, offset);
    }

    public void resetAccumulator(LagWithRetractAccumulator<T> acc) throws Exception {
        acc.offset = 1;
        acc.defaultValue = null;
        acc.buffer.clear();
    }

    @Override
    public T getValue(LagWithRetractAccumulator<T> acc) {
        if (acc.buffer.size() < acc.offset + 1) {
            LOG.info(acc.uniqueID() + ": Returning default value since size is " + acc.buffer.size());
            return acc.defaultValue;
        }
        int n = acc.buffer.size() - acc.offset - 1;
        LOG.info(acc.uniqueID() + ": Returning " + n + " index since size is " + acc.buffer.size() + " with " + acc.offset);

        if (n < acc.buffer.size()) {
            int count = 0;
            for (T element : acc.buffer) {
                if (n == count) {
                    LOG.info(acc.uniqueID() + ": Returning " + element.toString());
                    return element;
                }
                count++;
            }
        }
        return null;
    }

    @Override
    public LagWithRetractAccumulator<T> createAccumulator() {
        LOG.info("Creating a new accumulator....");
        return new LagWithRetractAccumulator<>();
    }


}
