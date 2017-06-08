package formats;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import lombok.Getter;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

import java.util.ArrayList;

@JsonIgnoreProperties(ignoreUnknown = true)
@DefaultCoder(AvroCoder.class)
public @Getter
class Click {
    public
    @Nullable
    Instant ts;
    public
    @Nullable
    String timestamp = "";
    public
    @Nullable
    String lsid = "";


    public TableRow toTableRow() {

        return new TableRow()
                .set("ts", ts.getMillis()/1000)
                .set("timestamp", timestamp)
                .set("lsid", lsid);
    }

    public static final ArrayList<TableFieldSchema> getTableFieldSchema() {
        ArrayList<TableFieldSchema> tableFieldSchema = new ArrayList<>();
        tableFieldSchema.add(new TableFieldSchema().setName("ts").setType("TIMESTAMP"));
        tableFieldSchema.add(new TableFieldSchema().setName("timestamp").setType("STRING"));
        tableFieldSchema.add(new TableFieldSchema().setName("lsid").setType("STRING"));
        return tableFieldSchema;
    }
}