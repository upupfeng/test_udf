package com.mwf.demo;

import com.sun.tools.corba.se.idl.constExpr.EvaluationException;
import jdk.nashorn.internal.ir.CallNode;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Date: 2019/3/3 12:43
 */
public class SimpleUDF extends UDF {
    public String evaluate(String str) {
        return str.toUpperCase();
    }
}

