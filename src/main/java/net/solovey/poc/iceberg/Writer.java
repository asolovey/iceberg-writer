package net.solovey.poc.iceberg;

import org.apache.iceberg.*;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.*;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Command(name = "iceberg-writer", mixinStandardHelpOptions = true,
        description = "POC of writing to S3-backed Iceberg table with JDBC catalog.")
public class Writer implements Callable<Integer> {
    @Option(
        names = "--catalog-uri",
        required = true,
        description = "JDBC catalog URI"
    )
    String catalogUri;

    @Option(
        names = "--catalog-user",
        description = "JDBC catalog user"
    )
    String catalogUser;

    @Option(
        names = "--catalog-password",
        description = "JDBC catalog password"
    )
    String catalogPassword;

    @Option(
        names = "--bucket",
        required = true,
        description = "Warehouse S3 bucket"
    )
    String bucket;

    @Option(
        names = "--prefix"
    )
    String prefix;

    @Option(
        names = "--table",
        required = true
    )
    String tableName;

    @Option(
        names = "--namespace",
        defaultValue = ""
    )
    String namespace;

    @Option(
        names = "--profile"
    )
    String awsProfile;

    @Option(
        names = "--s3-endpoint-url"
    )
    String s3endpointUrl;

    static final Schema schema_;

    static {
        schema_ = new Schema(
            Types.NestedField.of(1, false, "id", Types.IntegerType.get()),
            Types.NestedField.of(2, false, "data", Types.StringType.get()),
            Types.NestedField.of(3, false, "part", Types.StringType.get())
        );
    }

    Catalog catalog_;
    Table table_;

    public void initCatalog() {
        final HashMap<String, String> catalogProperties = new HashMap<>();

        catalogProperties.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        String warehouse = "s3://" + bucket;
        if (prefix != null && !prefix.isBlank()) {
            warehouse += prefix;
        }
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        catalogProperties.put(CatalogProperties.URI, catalogUri);
        catalogProperties.put(JdbcCatalog.PROPERTY_PREFIX + "user", catalogUser);
        catalogProperties.put(JdbcCatalog.PROPERTY_PREFIX + "password", catalogPassword);

        catalogProperties.put(S3FileIOProperties.ENDPOINT, s3endpointUrl);
        catalogProperties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");

        catalog_ = CatalogUtil.loadCatalog(
                JdbcCatalog.class.getName(),
                "jdbc",
                catalogProperties,
                null
        );
    }

    public void initTable() {
        var identifier = TableIdentifier.of(namespace, tableName);
        if (catalog_.tableExists(identifier)) {
            table_ = catalog_.loadTable(identifier);
        }
        else {
            table_ = catalog_.buildTable(identifier, schema_)
                .withPartitionSpec(PartitionSpec.builderFor(schema_).identity("part").build())
                .create()
            ;
        }
    }

    @Override
    public Integer call() throws IOException {
        if (awsProfile != null && !awsProfile.isBlank()) {
            System.setProperty("aws.profile", awsProfile);
        }

        initCatalog();
        initTable();

        List<Record> records = new ArrayList<>();
        var record = GenericRecord.create(schema_);
        records.add(record.copy(Map.of("id", 1, "data", "one", "part", "a")));
        records.add(record.copy(Map.of("id", 2, "data", "two", "part", "a")));
        records.add(record.copy(Map.of("id", 3, "data", "three", "part", "a")));
        records.add(record.copy(Map.of("id", 4, "data", "four", "part", "a")));

        PartitionKey partitionKey = new PartitionKey(table_.spec(), table_.schema());
        partitionKey.partition(records.get(0));

        var outputfileFactory = OutputFileFactory.builderFor(table_, 1, 1).format(FileFormat.PARQUET).build();
        var appenderFactory = new GenericAppenderFactory(table_.schema(), table_.spec());

        TaskWriter<Record> taskWriter = new MyTaskWriter(
            table_.spec(),
            FileFormat.PARQUET,
            appenderFactory,
            outputfileFactory,
            table_.io(),
            1024*1024,
            partitionKey
        );

        for (var r : records) {
            taskWriter.write(r);
        }

        var result = taskWriter.complete();

        var append = table_.newAppend();
        Tasks.foreach(result.dataFiles()).run(append::appendFile, IOException.class);
        append.commit();
        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new Writer()).execute(args);
        System.exit(exitCode);
    }
}

class MyTaskWriter extends BaseTaskWriter<Record> {
    private RollingFileWriter dataWriter;

    public MyTaskWriter(
        PartitionSpec spec,
        FileFormat format,
        FileAppenderFactory<Record> appenderFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        PartitionKey partitionKey
    ) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.dataWriter = new RollingFileWriter(partitionKey);
    }

    @Override
    public void write(Record row) throws IOException {
        dataWriter.write(row);
    }

    @Override
    public void close() throws IOException {
        if (dataWriter != null) {
            dataWriter.close();
        }
    }
}
