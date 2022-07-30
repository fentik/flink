/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.ExceptionUtils;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
//import org.apache.flink.table.types.utils.TypeConversions;

import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;

/**
 * An operator that maintains incoming records in state corresponding to the upsert keys and
 * generates an upsert view for the downstream operator.
 *
 * <ul>
 *   <li>Adds an insertion to state and emits it with updated {@link RowKind}.
 *   <li>Applies a deletion to state.
 *   <li>Emits a deletion with updated {@link RowKind} iff affects the last record or the state is
 *       empty afterwards. A deletion to an already updated record is swallowed.
 * </ul>
 */
public class SinkUpsertMaterializer extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SinkUpsertMaterializer.class);

    private static final String STATE_CLEARED_WARN_MSG =
            "The state is cleared because of state ttl. This will result in incorrect result. "
                    + "You can increase the state ttl to avoid this.";

    private final StateTtlConfig ttlConfig;
    private final TypeSerializer<RowData> serializer;
    private final GeneratedRecordEqualiser generatedEqualiser;

    private transient RecordEqualiser equaliser;
    private transient RowType physicalRowType;

    // Buffer of emitted insertions on which deletions will be applied first.
    // The row kind might be +I or +U and will be ignored when applying the deletion.
    private transient ValueState<List<RowData>> state;
    private transient TimestampedCollector<RowData> collector;

    public SinkUpsertMaterializer(
            StateTtlConfig ttlConfig,
            TypeSerializer<RowData> serializer,
            GeneratedRecordEqualiser generatedEqualiser) {
        this.ttlConfig = ttlConfig;
        this.serializer = serializer;
        this.generatedEqualiser = generatedEqualiser;
        this.physicalRowType = null;
        LOG.info("WARNING: SinkUpsertMaterializer is being called without the physicalRowType - This reduces what we can log for debugging. See the stacktrace to see if you can fix the caller.");
        String stacktrace = ExceptionUtils.stringifyException((new Exception("Missing RowType information")));
        LOG.info(stacktrace);
    }

    public SinkUpsertMaterializer(
        StateTtlConfig ttlConfig,
        TypeSerializer<RowData> serializer,
        GeneratedRecordEqualiser generatedEqualiser,
        RowType physicalRowType) {
        this.ttlConfig = ttlConfig;
        this.serializer = serializer;
        this.generatedEqualiser = generatedEqualiser;
        this.physicalRowType = physicalRowType;
        if (this.physicalRowType == null) {
            LOG.info("WARNING: SinkUpsertMaterializer is being called without the physicalRowType - This reduces what we can log for debugging. See the stacktrace to see if you can fix the caller.");
            String stacktrace = ExceptionUtils.stringifyException((new Exception("Missing RowType information")));
            LOG.info(stacktrace);
        } else {
            LOG.info("Initializing SinkUpsertMaterializer with schema for the input row: " + this.physicalRowType.asSummaryString());
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.equaliser =
                generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        ValueStateDescriptor<List<RowData>> descriptor =
                new ValueStateDescriptor<>("values", new ListSerializer<>(serializer));
        if (ttlConfig.isEnabled()) {
            descriptor.enableTimeToLive(ttlConfig);
        }
        this.state = getRuntimeContext().getState(descriptor);
        this.collector = new TimestampedCollector<>(output);
    }

    // TODO(akhilg): This is an exact copy of a function in AbstractStreamingOperator. When you are getting
    // bored, figure out which class/interface can be put this function in and re-use between these two classes.
    protected static String rowToString(RowType type, RowData row) {
        LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        int rowArity = type.getFieldCount();
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


    public String patreonPRMToString(RowData row) {
        return "agg_key " + row.getString(0).toString() +  // agg_key
               ", campaign_id " + row.getLong(1) +  //campaign_id
               ", patron_id " + row.getLong(2) +  //patron_id
               ", billing_cycle_id " + row.getInt(3) +  // billing_cycle_id
               row.getInt(4) +  // is_follower
               row.getString(5).toString() + // member_uuid
               row.getString(6).toString() + //thumb_url
               row.getString(7).toString() + // vanity
               row.getString(8).toString() + // full_name
               row.getString(9).toString() + // email
               row.getString(10).toString() + //discord
               " " + row.getBoolean(11)  +  //in_good_standing
               " " + row.getBoolean(12) +  // is_follower0
               " " + row.getBoolean(13) +  // is_cuf
               " " + row.getBoolean(14) +  // is_annual
               " " + row.getShort(15)  +    //user_is_deleted
               row.getShort(16) +// user_is_nuked
               row.getShort(17) +  // user_is_disabled
               row.getBoolean(18) + //user_is_blocked
               row.getBoolean(19) + // creator_is_blocked
               row.getBoolean(20) +  // is_member
               row.getString(21).toString() + // currency
               row.getString(22).toString() + // campaign_currency
               row.getLong(23) + // pledge_amount_cents
               row.getInt(24) + // campaign_pledge_amount_cents
               row.getLong(25) + // pledge_cap_amount_cents
               row.getLong(26) + // lifetime_support_cents
               row.getInt(27) + // campaign_lifetime_support_cents
               row.getLong(28) + // will_pay_amount_cents
               row.getString(29).toString() +  // pledge_status
               row.getString(30).toString() +  //latest_bill_status
               row.getTimestamp(31, 6).toString() + //latest_bill_failed_at
               ", latest_bill_succeeded_at " + row.getTimestamp(32, 6).toString() + // latest_bill_succeeded_at
               ", latest_bill_created_at " + row.getTimestamp(33, 6).toString() + // latest_bill_created_at
               row.getString(34).toString() + //latest_patronage_purchase_status
               // latest_patronage_purchase_created_at
               ", latest_patronage_purchase_created_at " + row.getTimestamp(35, 6).toString() +
               // latest_patronage_purchase_settled_at
               ", latest_patronage_purchase_settled_at " + row.getTimestamp(36, 6).toString() +
               row.getLong(37) + // pledge_id
               row.getLong(38) + // pledge_cadence
               row.getTimestamp(39, 6).toString() + // pledge_relationship_start
               row.getTimestamp(40, 6).toString() + // pledge_relationship_end
               ", last_charge_date " + row.getTimestamp(41, 6).toString() + // last_charge_date
               row.getString(42).toString() +// last_charge_status
               row.getTimestamp(43, 6).toString() + // next_charge_date
               ", reward_id " + row.getLong(44) + // reward_id
               row.getLong(45) + // reward_amount_cents
               row.getString(46).toString() + //reward_description
               row.getShort(47) +  // reward_requires_shipping
               row.getString(48).toString() + // reward_title
               row.getString(49).toString() + //benefit_ids
               row.getShort(50) + // shipping_opt_out
               row.getLong(51) + // shipping_address_id
               row.getString(52).toString() + // shipping_address_name
               row.getString(53).toString() +  // shipping_address_addressee
               row.getString(54).toString() +  // shipping_address_line_1
               row.getString(55).toString() +  // shipping_address_line_2
               row.getString(56).toString() +  // shpping_adddress_postal_code
               row.getString(57).toString() + // shipping_address_city
               row.getString(58).toString() + // shipping_address_state
               row.getString(59).toString() + // shipping_address_country
               // Don't get shpping_address_coordinates
               row.getString(61).toString() +  // shipping_address_phone_number
               row.getString(62).toString();   //notes
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        final RowData row = element.getValue();
        List<RowData> values = state.value();
        if (values == null) {
            values = new ArrayList<>(2);
        }
        int size = -1;
        String key = row.getString(0).toString();  // Assumes the first column in a string.
        if (row instanceof BinaryRowData) {
            size = ((BinaryRowData)row).getSizeInBytes();
        }
        if (values.size() > 10 || size > 1024*1024) {
            LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: EXPENSIVE update to state value with key " + key + " to values {num entries: " + values.size() + "} with a row of size " + size);
        }
        if (this.shouldLogInput()) {
            if (this.physicalRowType != null) {
                LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: Processing input (" + row.getRowKind() + ") with key " + key + " (" + size + ")" + " to values {num entries: " + values.size() + "} : " +  this.rowToString(this.physicalRowType, row));
            } else {
//                LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: Processing input (" + row.getRowKind() + ") with key " + key + "(" + size + ")" + " to values {num entries: " + values.size() + "} : " + element.toString());
                LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: Processing input (" + row.getRowKind() + ") with value: " + patreonPRMToString(row) + " (" + size + ")" + " to values {num entries: " + values.size() + "}");
            }
        }
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                row.setRowKind(values.isEmpty() ? INSERT : UPDATE_AFTER);
                values.add(row);
                collector.collect(row);
                break;

            case UPDATE_BEFORE:
            case DELETE:
                final int lastIndex = values.size() - 1;
                final int index = removeFirst(values, row);
                if (index == -1) {
                    LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: WARNING: Did not find a value for " + key + ".We may have truncated the values too early.");
                    LOG.info(STATE_CLEARED_WARN_MSG);
                    return;
                }
                if (values.isEmpty()) {
                    // Delete this row
                    row.setRowKind(DELETE);
                    collector.collect(row);
                } else if (index == lastIndex) {
                    // Last row has been removed, update to the second last one
                    final RowData latestRow = values.get(values.size() - 1);
                    latestRow.setRowKind(UPDATE_AFTER);
                    collector.collect(latestRow);
                }
                break;
        }

        if (values.isEmpty()) {
            state.clear();
        } else {
            // We assume we are not going to get out-of-order entries for more than 20.
//            if (values.size() > 50) {
//                LOG.info("[SubTask Id: (" + getRuntimeContext().getIndexOfThisSubtask() + ")]: Removing 30 entries for " + key + " currently with " + values.size() + " values. Hopefully, this doesn't cause issues.");
              //  removeN(values, 30);
         //   }
            state.update(values);
        }
    }

    private void removeN(List<RowData> values, int count) {
        for(int i = 0; i < count; i++) {
            values.remove(0);
        }
    }

    private int removeFirst(List<RowData> values, RowData remove) {
        final Iterator<RowData> iterator = values.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            final RowData row = iterator.next();
            // Ignore kind during comparison
            remove.setRowKind(row.getRowKind());
            if (equaliser.equals(row, remove)) {
                iterator.remove();
                return i;
            }
            i++;
        }
        return -1;
    }
}
