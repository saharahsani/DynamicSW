package clustream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class AdjacencyMatrix {
    public static <Integer> ArrayList<Integer> removeDuplicates(ArrayList<Integer> list)
    {

        // Create a new LinkedHashSet
        Set<Integer> set = new LinkedHashSet<>();

        // Add the elements to set
        set.addAll(list);

        // Clear the list
        list.clear();

        // add the elements of set
        // with no duplicates to the list
        list.addAll(set);

        // return the list
        return list;
    }
    public static final String getRealId = "getRealId";
    public static Dataset<Row> computeGraph(List<Row> lRows, SparkSession spark) {
        ArrayList<Integer> lIds = new ArrayList<Integer>();
        List<Row> lIdWithIdx = new ArrayList<Row>();

        for (int i = 0; i <lRows.size() ; i++) {
            Integer mcId = lRows.get(i).getAs("mcId");
            Integer nearId = lRows.get(i).getAs("nearId");
            lIds.add(mcId);
            lIds.add(nearId);
        }
      //  ArrayList<Integer> ff=removeDuplicates(lIds);
       //Integer dd= removeDuplicates(lIds).size();
        Graph g = new Graph( removeDuplicates(lIds).size());
        for (int i = 0; i < lRows.size(); i++) {
            Integer mcId = lRows.get(i).getAs("mcId");
            Integer nearId = lRows.get(i).getAs("nearId");
            g.addEdge(lIds.indexOf(mcId), lIds.indexOf(nearId));
           // g.addEdge(mcId,nearId);

           /* Row r1 = RowFactory.create(mcId, lIds.indexOf(mcId));
            Row r2 = RowFactory.create(nearId, lIds.indexOf(nearId));
            lIdWithIdx.add(r1);
            lIdWithIdx.add(r2);*/

        }
        // change to df get index
        /*List<StructField> lSF = new ArrayList<>();
        lSF.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        lSF.add(DataTypes.createStructField("index", DataTypes.IntegerType, true));
        StructType sType = DataTypes.createStructType(lSF);
        Dataset<Row> dfIdWithIndex = spark.createDataFrame(lIdWithIdx, sType);*/
       // dfIdWithIndex.show();
        // get res
        List<Row> listOfRow = g.getResult(g);

        List<StructField> listOfStructField = new ArrayList<>();
        listOfStructField.add(DataTypes.createStructField("NEW_CLUSTER_NUMBER", DataTypes.IntegerType, true));
        listOfStructField.add(DataTypes.createStructField("CLUSTER_NUMBER", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(listOfStructField);
        Dataset<Row> dfWithNewId = spark.createDataFrame(listOfRow, structType);

        spark.udf().register("getRealId", (UDF1<Integer, Integer>)
                (clusterIdx) -> {
            Integer realId=lIds.get(clusterIdx);
                    return realId;
                }, DataTypes.IntegerType);

      Dataset<Row> dfWithRealId= dfWithNewId.withColumn("realId", callUDF(getRealId,col("CLUSTER_NUMBER")));

        //dfWithRealId.show();
        return dfWithRealId.select(col("NEW_CLUSTER_NUMBER"),col("realId").as("CLUSTER_NUMBER"));//spark.createDataFrame(listOfRow, structType);


    }


}
