package export;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.graphdb.schema.*;
import org.neo4j.procedure.*;
import org.neo4j.logging.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

/**
 * This will extract a Neo4j database's graph
 */
public class ExtractGraph {
  @Context
  public GraphDatabaseService db;

  private static int chunkSize = 25000;

  @UserFunction
  @Description("Extract graph of current database")
  public void extract_graph(File file, String[] values) throws IOException {
    try ( Transaction tx = db.beginTx() ) {

      ArrayList<VectorSchemaRoot> schemaRoots = new ArrayList();
      ArrayList<FileOutputStream> fds = new ArrayList();
      ArrayList<ArrowFileWriter> fileWriters = new ArrayList();

      // extract nodes first
      try {
        // get all property keys in use
        

        for (ArrowFileWriter fw : fileWriters) {
          fw.start();
        }

        ResourceIterator<Node> nodes = tx.getAllNodes().iterator();
        long index = 0;

        while (nodes.hasNext()) {
          for (VectorSchemaRoot sr : schemaRoots) {
            sr.allocateNew();
          }

          int chunkIndex = 0;
          while (chunkIndex < chunkSize && nodes.hasNext()) {
            Node node = nodes.next();
            int outdegree = node.getDegree(Direction.OUTGOING);
            //Iterator<Label> labels = node.getLabels().iterator();
            labels.close();
            Map<String, Object> properties = node.getAllProperties();
            chunkIndex++;
          }


          for (VectorSchemaRoot sr : schemaRoots) {
            sr.setRowCount(chunkIndex);
          }
          for (ArrowFileWriter fw : fileWriters) {
            fw.writeBatch();
          }

          index += chunkIndex;
          for (VectorSchemaRoot sr : schemaRoots) {
            sr.clear();
          }
        }

        for (ArrowFileWriter fw : fileWriters) {
          fw.end();
        }

        nodes.close();
      } finally {
        // close and then clear lists TODO
        schemaRoots.clear();
        fds.clear();
        fileWriters.clear();
      }
      tx.commit();

      // extract edges
    }
  }

  // from APOC TODO
  private static DecimalFormat decimalFormat = new DecimalFormat() {
    {
        setMaximumFractionDigits(340);
        setGroupingUsed(false);
        setDecimalSeparatorAlwaysShown(false);
        DecimalFormatSymbols symbols = getDecimalFormatSymbols();
        symbols.setDecimalSeparator('.');
        setDecimalFormatSymbols(symbols);
    }
  };
  public static String formatNumber(Number value) {
    if (value == null) return null;
    return decimalFormat.format(value);
  }

  public static String formatString(Object value) {
      return "\"" + String.valueOf(value).replaceAll("\\\\", "\\\\\\\\")
              .replaceAll("\n","\\\\n")
              .replaceAll("\r","\\\\r")
              .replaceAll("\t","\\\\t")
              .replaceAll("\"","\\\\\"") + "\"";
  }
  public static String toString(Object value) {
      if (value == null) return "";
      if (value.getClass().isArray()) {
          return toJson(value);
      }
      if (value instanceof Number) {
          return formatNumber((Number)value);
      }
      if (value instanceof Point) {
          return formatPoint((Point) value);
      }
      return value.toString();
  }
  public static String formatPoint(Point value) {
      try {
          return JsonUtil.OBJECT_MAPPER.writeValueAsString(value);
      } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
      }
  }
  public static String toJson(Object value) {
    try {
        return JsonUtil.OBJECT_MAPPER.writeValueAsString(value);
    } catch (IOException e) {
        throw new RuntimeException("Can't convert "+value+" to JSON");
    }
}
}
