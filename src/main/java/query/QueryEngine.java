package query;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.HashSet;

import static org.apache.spark.sql.functions.*;

public class QueryEngine {

    private static final SparkSession spark;

    public static final String EMPTY_RESULT = "{}";

    static{

        //TODO add summary data

        spark = SparkSession.builder().appName("genetics-app").master("local[*]").getOrCreate();

        String awsKey = System.getProperty("AWS_ACCESS_KEY_ID");
        String awsSecret = System.getProperty("AWS_SECRET_ACCESS_KEY");

        if (awsKey != null && awsSecret != null) {
            Configuration conf = spark.sparkContext().hadoopConfiguration();
            conf.set("fs.s3a.access.key", awsKey);
            conf.set("fs.s3a.secret.key", awsSecret);
        }

    }

    public static String getMutationsByIndex(String chrom, int pos, String repoPath){

        String path = repoPath + String.format("chrom=%s/pos_bucket=%d/", "chr" + chrom.toUpperCase(), Math.floorDiv(pos, 1_000_000));

        Dataset result = spark
                .read().parquet(path)
                .where(col("pos").equalTo(pos))
                .select(to_json(struct(col("entries")))).cache();

        return result.count() == 0 ? EMPTY_RESULT : (String) result.as(Encoders.STRING()).collectAsList().get(0);
    }

    public static String getMutationsByRange(String chrom, int posFrom, int posTo, String repoPath, int maxRecordsNum){

        // for range queries we scan at most two buckets
        String path1 = repoPath + String.format("chrom=%s/pos_bucket=%d/", "chr" + chrom.toUpperCase(), Math.floorDiv(posFrom, 1_000_000));
        String path2 = repoPath + String.format("chrom=%s/pos_bucket=%d/", "chr" + chrom.toUpperCase(), Math.floorDiv(posTo, 1_000_000));


        Dataset df = spark.read().parquet(JavaConversions.asScalaSet(new HashSet(Arrays.asList(path1, path2))).toSeq());

        Dataset result = df
                .where(col("pos").geq(posFrom))
                .where(col("pos").leq(posTo))
                .orderBy("pos")
                .groupBy()
                .agg(
                        to_json(
                                struct(
                                        count("*").as("count"),
                                        slice(
                                                collect_list(
                                                        struct(col("pos"), col("entries"))),1, maxRecordsNum
                                        ).as("data")
                                )
                        )
                );

        return (String) result.as(Encoders.STRING()).collectAsList().get(0);
    }
}
