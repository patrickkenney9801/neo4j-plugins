package export;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.*;
import org.neo4j.procedure.*;
import org.neo4j.logging.*;

/**
 * This will extract a Neo4j database's indexes and constraints
 */
public class ExtractSchema {
    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("Extract schema of current database")
    public String extract_schema() {

        String output = "";
        try ( Transaction tx = db.beginTx() ) {
            Schema schema = tx.schema();
            Iterable<ConstraintDefinition> constraints = schema.getConstraints();
            Iterable<IndexDefinition> indexes = schema.getIndexes();

            for (ConstraintDefinition cd : constraints) {
                output += "Constraint: " + cd.getName() + ", Constraint Type: " + cd.getConstraintType();
                if (cd.getConstraintType() == ConstraintType.UNIQUENESS || cd.getConstraintType() == ConstraintType.NODE_PROPERTY_EXISTENCE) {
                    output += ", Label: " + cd.getLabel();
                } else {
                    output += ", Relationship Type: " + cd.getRelationshipType().name();
                }
                output += ", Keys: ";
                for (String key : cd.getPropertyKeys()) {
                    output += key + " ";
                }
                output += "\n";
            }
            for (IndexDefinition id : indexes) {
                output += "Index: " + id.getName() + ", Index Type: " + id.getIndexType();
                if (id.isNodeIndex()) {
                    output += ", Labels: ";
                    for (Label label : id.getLabels()) {
                        output += label + " ";
                    }
                } else if (id.isRelationshipIndex()) {
                    output += ", Relationship Types: ";
                    for (RelationshipType type : id.getRelationshipTypes()) {
                        output += type.name() + " ";
                    }
                }
                output += ", Keys: ";
                for (String key : id.getPropertyKeys()) {
                    output += key + " ";
                }
                output += "\n";
            }
            output += "End of Schema\n";
            tx.commit();
        }
        return output;
    }
}
