package export;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.graphdb.schema.*;
import org.neo4j.procedure.*;
import org.neo4j.logging.*;

//import apoc.util.*;
//import apoc.export.util.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Iterator;

/**
 * This will extract a Neo4j database's graph
 */
public class ExtractGraph {
  @Context
  public GraphDatabaseService db;

  // Note: - is an illegal character in property names for Neo4j
  @UserFunction
  @Description("export.extract_graph(directory) - extract graph of current database")
  public String extract_graph(@Name("directory") String directoryName) throws IOException {
    if (directoryName == null || directoryName.length()== 0) {
      return "No directory name specified";
    } else if (directoryName.contains("\\")) {
      return "No nested directories allowed";
    }
    // create directory if it does not exist
    File directory = new File(directoryName);
    if (!directory.exists()) {
      directory.mkdir();
    }
    // TODO delete all files in directory if it already existed

    final int chunkSize = 25000;
    String out = "";
    try ( Transaction tx = db.beginTx() ) {

      ArrayList<RootAllocator> allocators = new ArrayList();
      ArrayList<VectorSchemaRoot> schemaRoots = new ArrayList();
      ArrayList<FileOutputStream> fds = new ArrayList();
      ArrayList<ArrowFileWriter> fileWriters = new ArrayList();

      // extract nodes first
      try {
        // get all labels in use
        Iterable<Label> allLabels = tx.getAllLabelsInUse();
        Map<String, Integer> labelIndexes = new HashMap();
        for (Label l : allLabels) {
          labelIndexes.put(l.name(), fileWriters.size());
          RootAllocator alloc = new RootAllocator();
          allocators.add(alloc);
          VectorSchemaRoot sr = VectorSchemaRoot.create(makeLabelSchema(l), alloc);
          schemaRoots.add(sr);
          FileOutputStream fd = new FileOutputStream(new File(directoryName + File.separator + getLabelName(l) + ".parquet"));
          fds.add(fd);
          fileWriters.add(new ArrowFileWriter(sr, null, fd.getChannel()));
        }

        // get all property keys in use
        

        for (ArrowFileWriter fw : fileWriters) {
          fw.start();
        }
        ResourceIterator<Node> nodes = tx.getAllNodes().iterator();
        // Note: must extend null list if/when new fields are added
        ArrayList<Boolean> nullValues = new ArrayList();
        for (int i = 0; i < fileWriters.size(); i++) {
          nullValues.add(false);
        }
        long index = 0;

        while (nodes.hasNext()) {
          for (VectorSchemaRoot sr : schemaRoots) {
            sr.allocateNew();
          }

          int chunkIndex = 0;
          while (chunkIndex < chunkSize && nodes.hasNext()) {
            Node node = nodes.next();
            //out += "Node:\n";
            int outdegree = node.getDegree(Direction.OUTGOING);
            Iterable<Label> labels = node.getLabels();
            for (Label l : labels) {
              int labelIndex = labelIndexes.get(l.name());
              nullValues.set(labelIndex, true);
              ((BitVector) schemaRoots.get(labelIndex).getVector(l.name())).setSafe(chunkIndex, 1);
            }
            /*
            Map<String, Object> properties = node.getAllProperties();
            for (Map.Entry<String, Object> e : properties.entrySet()) {
              out += e.getKey() + ": " + FormatUtils.toString(e.getValue()) + ", is type: " + e.getValue().getClass() +"\n";
            }
            out += "\n";
            */
            
            // add appropriate null values
            for (int i = 0; i < fileWriters.size(); i++) {
              if (!nullValues.get(i)) {
                // add null to index
                FieldVector vec = schemaRoots.get(i).getVector(0);
                if (vec instanceof BaseFixedWidthVector) {
                  ((BaseFixedWidthVector) vec).setNull(chunkIndex);
                } else {
                  out += "A value was not set to null\n";
                }
              } else {
                // reset null list
                nullValues.set(i, false);
              }
            }

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
        allocators.clear();
        schemaRoots.clear();
        fds.clear();
        fileWriters.clear();
      }
      tx.commit();

      // extract edges
    }
    return out;
  }

  private void addColumn(ArrayList<VectorSchemaRoot> schemaRoots, ArrayList<FileOutputStream> fds,
                         ArrayList<ArrowFileWriter> fileWriters, long index, String filename) {
    // add a column to the output, populate with appropriate amount of nulls first
  }

  private String getLabelName(Label label) {
    return "label-" + label.name();
  }

  private org.apache.arrow.vector.types.pojo.Schema makeLabelSchema(Label l) {
    ArrayList<Field> fields = new ArrayList();
    fields.add(new Field(l.name(), FieldType.nullable(new ArrowType.Bool()), null));
    return new org.apache.arrow.vector.types.pojo.Schema(fields);
  }
}