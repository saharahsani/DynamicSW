package clustream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class AdjacencyMatrixOld {
    public static Dataset<Row> computeGraph(List<Row> lRows, Integer size, SparkSession spark) {

            Graph g = new Graph(size);
            for (int i = 0; i < lRows.size(); i++) {
              Integer mcId=  lRows.get(i).getAs("mcId");
                Integer nearId=  lRows.get(i).getAs("nearId");
                g.addEdge(mcId, nearId);
            }

           List<Row> listOfRow= g.getResult(g);
            List<StructField> listOfStructField = new ArrayList<>();
            listOfStructField.add(DataTypes.createStructField("NEW_CLUSTER_NUMBER", DataTypes.IntegerType, true));
            listOfStructField.add(DataTypes.createStructField("CLUSTER_NUMBER", DataTypes.IntegerType, true));
            StructType structType = DataTypes.createStructType(listOfStructField);

            return spark.createDataFrame(listOfRow, structType);


    }
}
