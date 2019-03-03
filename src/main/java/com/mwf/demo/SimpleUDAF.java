package com.mwf.demo;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * @Date: 2019/3/3 12:04
 */
public class SimpleUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        return new TestEvaluator();
    }

    public static class TestEvaluator extends GenericUDAFEvaluator{

        //数据的类型
        PrimitiveObjectInspector inputOI;
        ObjectInspector ouputOI;
        PrimitiveObjectInspector integerOI;

        //总数
        int total = 0 ;

        //指定输入，过程和输出的类型(有不同的阶段PARTIAL1,PARTIAL2,FINAL,COMPLETE)
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if(m ==Mode.PARTIAL1 || m == Mode.COMPLETE ){
                inputOI = (PrimitiveObjectInspector) parameters[0];
            }else{
                integerOI = (PrimitiveObjectInspector) parameters[0];
            }
            ouputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return ouputOI;
        }

        //聚集结果存储的中间类
        static class HobbyAggregationBuffer implements AggregationBuffer {
            int tempTotal = 0 ;
            void add(int num){tempTotal += num; }
        }

        //获取聚集结果存储的中间类
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new HobbyAggregationBuffer();
        }

        //重置聚集结果存储的中间类
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            aggregationBuffer = new HobbyAggregationBuffer();
        }

        //对每一行进行逻辑计算
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            if(objects[0] != null ){
                HobbyAggregationBuffer hobbyAgg = (HobbyAggregationBuffer) aggregationBuffer;
                Object p = ((PrimitiveObjectInspector)inputOI).getPrimitiveJavaObject(objects[0]);
                hobbyAgg.add(String.valueOf(p).split(",").length);
            }
        }

        //对中间值(mapper和combiner的结果)进行计算
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            HobbyAggregationBuffer hobbyAgg = (HobbyAggregationBuffer) aggregationBuffer;
            total += hobbyAgg.tempTotal;
            return total;
        }

        //合并mapper阶段和combiner阶段的结果
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            if(o != null){
                HobbyAggregationBuffer hobbyAgg1 = (HobbyAggregationBuffer) aggregationBuffer;
                Integer partialSum = (Integer) integerOI.getPrimitiveJavaObject(o);
                HobbyAggregationBuffer hobbyAgg2 = new HobbyAggregationBuffer();
                hobbyAgg2.add(partialSum);
                hobbyAgg1.add(hobbyAgg2.tempTotal);
            }


        }

        //对结果值(reducer的记过)进行计算
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            HobbyAggregationBuffer hobbyAgg = (HobbyAggregationBuffer) aggregationBuffer;
            total = hobbyAgg.tempTotal;
            return total;
        }
    }
}
