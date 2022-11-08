package com.fentik;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.List;

public class array_cat extends ScalarFunction {

    public List<String> eval(List<String> ar1, List<String> ar2) {
        if (ar1 == null || ar2 == null) {
            return null;
        }

        ArrayList<String> res = new ArrayList<String>();

        for (String v : ar1) {
            res.add(v);
        }

        for (String v : ar2) {
            res.add(v);
        }

        return res;
    }

    public List<String> eval(List<String> ar1, String argScalar) {
        if (ar1 == null || argScalar == null) {
            return null;
        }

        ArrayList<String> res = new ArrayList<String>(ar1);
        res.add(argScalar);

        return res;
    }
}
