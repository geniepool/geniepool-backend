package query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class QueryEngine {

    private static final SparkSession spark;
    private static final int PARTITION_SIZE = 100_000;
    private static final int PARTITION_SIZE_DBSNP = 10_000_000;

    static{

        SparkConf sparkConf = new SparkConf()
                .setAppName("genetics-app")
                .setMaster("local[*]")
                .set("spark.log.level", "WARN");

        spark = SparkSession.builder().config(sparkConf).getOrCreate();

        String awsKey = System.getProperty("AWS_ACCESS_KEY_ID");
        String awsSecret = System.getProperty("AWS_SECRET_ACCESS_KEY");

        if (awsKey != null && awsSecret != null) {
            Configuration conf = spark.sparkContext().hadoopConfiguration();
            conf.set("fs.s3a.access.key", awsKey);
            conf.set("fs.s3a.secret.key", awsSecret);
        }
    }

    public static String getMutationsByRange(String chrom, int posFrom, int posTo, String repoPath, int maxRecordsNum, Double am, Integer qual, Integer ad){

        // for range queries we scan at most two buckets
        String path1 = repoPath + String.format("chrom=%s/pos_bucket=%d/", "chr" + chrom.toUpperCase(), Math.floorDiv(posFrom, PARTITION_SIZE));
        String path2 = repoPath + String.format("chrom=%s/pos_bucket=%d/", "chr" + chrom.toUpperCase(), Math.floorDiv(posTo, PARTITION_SIZE));

        Dataset df = spark.read().parquet(JavaConversions.asScalaSet(new HashSet(Arrays.asList(path1, path2))).toSeq());

        Dataset result = df
                .where(col("pos").geq(posFrom))
                .where(col("pos").leq(posTo));

        if (am != null && am > 0){
            result = result
                    .filter(expr(String.format("exists(entries, x -> x.alphamissense > %f)", am)))
                    .withColumn("entries", expr(String.format("filter(entries,  x -> x.alphamissense >= %f)", am)));
        }

        if (qual != null) {
            result = result
                    .filter(expr(String.format("exists(entries, x -> exists (x.het, y -> y.qual >= %d))", qual))
                            .or(expr(String.format("exists(entries, x -> exists (x.hom, y -> y.qual >= %d))", qual))));
        }

        if (ad != null) {
            result = result
                    .filter(expr(String.format("exists(entries, x -> exists (x.het, y -> split(y.ad, ',')[0] + split(y.ad, ',')[1] >= %d))", ad)).or(
                            expr(String.format("exists(entries, x -> exists (x.hom, y -> split(y.ad, ',')[0] + split(y.ad, ',')[1] >= %d))", ad))
                    ));
        }

        result = result
                .orderBy("pos")
                .groupBy()
                .agg(
                        to_json(
                                struct(
                                        coalesce(sum(size(col("entries"))), lit(0)).as("count"),
                                        slice(
                                                collect_list(
                                                        struct(lit(chrom).as("chrom"), col("pos"), col("entries"))),1, maxRecordsNum
                                        ).as("data")
                                )
                        )
                );

        return (String) result.as(Encoders.STRING()).collectAsList().get(0);
    }

    public static String getRepoStatus(String statusPath){

        return spark.read()
                .json(statusPath)
                .orderBy(col("update_date").desc())
                .limit(1)
                .select(
                        to_json(
                            struct(
                                    "mutations_num", "samples_num", "update_date"
                            )
                        )
                ).as(Encoders.STRING()).collectAsList().get(0);

    }

    public static Pair<String, String> dbsnpToCoordinate(String dbSnpPath, String dbsnpId){
        List<Row> result;

        try {
            String filePath = dbSnpPath + String.format("part=%d/", Math.floorDiv(Integer.parseInt(dbsnpId), PARTITION_SIZE_DBSNP));
            Dataset dbsnpIndex = spark.read().parquet(filePath).where(col("id").equalTo(dbsnpId));
            result = dbsnpIndex.collectAsList();
        }catch(Exception e){
            return null;
        }

        if (!result.isEmpty()){
            String chrom = result.get(0).getAs("chrom");
            Long pos = result.get(0).getAs("pos");
            return Pair.of(chrom.toLowerCase(), String.valueOf(pos));
        }else {
            return null;
        }
    }

}
