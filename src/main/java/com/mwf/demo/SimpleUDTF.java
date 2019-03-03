package com.mwf.demo;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

/**
 * @Date: 2019/3/3 11:55
 */
public class SimpleUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        ArrayList<String> colNames = new ArrayList<String>();
        colNames.add("hobby1");
        colNames.add("hobby2");
        ArrayList<ObjectInspector> fieldIOs = new ArrayList<ObjectInspector>();
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(colNames,fieldIOs);
    }

    public void process(Object[] objects) throws HiveException {
        forward(objects[0].toString().split(","));
    }

    public void close() throws HiveException {

    }
}
