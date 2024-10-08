package rest;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import query.QueryEngine;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static javax.ws.rs.core.Response.Status.*;
import static query.QueryEngine.dbsnpToCoordinate;


@Path("/index")
public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);

    private static final Set<String> VALID_CHROMOSOMES = new HashSet<>(Arrays.asList(
            "1", "2", "3","4","5","6", "7", "8", "9","10",
            "11", "12", "13","14","15","16", "17", "18", "19", "20",
            "21", "22", "23", "x", "y", "m"
    ));

    private static final String HG19_PATH;
    private static final String HG38_PATH;
    private static final String CHM13V2_PATH;

    private static final String HG19_STATUS_PATH;
    private static final String HG38_STATUS_PATH;
    private static final String CHM13V2_STATUS_PATH;

    private static final String HG19_DBSNP_PATH;
    private static final String HG38_DBSNP_PATH;
    private static final String CHM13V2_DBSNP_PATH;

    private static final int MAX_RANGE_RECORDS_IN_RESULT;

    static{
        HG19_PATH = System.getProperty("REPO_HG_19_PATH");
        HG38_PATH = System.getProperty("REPO_HG_38_PATH");
        CHM13V2_PATH = System.getProperty("REPO_CHM13V2_PATH");

        HG19_STATUS_PATH = System.getProperty("HG19_STATUS_PATH");
        HG38_STATUS_PATH = System.getProperty("HG38_STATUS_PATH");
        CHM13V2_STATUS_PATH = System.getProperty("CHM13V2_STATUS_PATH");

        HG19_DBSNP_PATH = System.getProperty("HG19_DBSNP_PATH");
        HG38_DBSNP_PATH = System.getProperty("HG38_DBSNP_PATH");
        CHM13V2_DBSNP_PATH = System.getProperty("CHM13V2_DBSNP_PATH");

        if (HG19_PATH == null || HG19_PATH.isEmpty() || HG38_PATH == null
                || HG38_PATH.isEmpty() || HG19_STATUS_PATH == null || HG19_STATUS_PATH.isEmpty()
                || HG38_STATUS_PATH == null || HG38_STATUS_PATH.isEmpty() || CHM13V2_PATH == null || CHM13V2_PATH.isEmpty()
                || CHM13V2_STATUS_PATH == null || CHM13V2_STATUS_PATH.isEmpty() || HG19_DBSNP_PATH == null || HG19_DBSNP_PATH.isEmpty()
                || HG38_DBSNP_PATH == null || HG38_DBSNP_PATH.isEmpty() || CHM13V2_DBSNP_PATH == null || CHM13V2_DBSNP_PATH.isEmpty()

        ){
            throw new IllegalStateException("repo or status path is empty!");
        }

        MAX_RANGE_RECORDS_IN_RESULT = Integer.parseInt(System.getProperty("MAX_RANGE_RECORDS_IN_RESULT", "100"));
    }

    @GET
    @Path("/hg38/{index}")
    public Response getResult38(@PathParam("index") String index, @QueryParam("am") Double am, @QueryParam("qual") Integer qual, @QueryParam("ad") Integer ad) {
        return getResult(index, HG38_PATH, HG38_DBSNP_PATH, MAX_RANGE_RECORDS_IN_RESULT, am, qual, ad);
    }

    @GET
    @Path("/hg38/status")
    public Response getStatus38() {
        try{
            String status = QueryEngine.getRepoStatus(HG38_STATUS_PATH);
            return Response.status(OK).entity(status).build();
        }catch(Exception e){
            logger.error(e);
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @GET
    @Path("/hg19/{index}")
    public Response getResult19(@PathParam("index") String index, @QueryParam("am") Double am, @QueryParam("qual") Integer qual, @QueryParam("ad") Integer ad) {
        return getResult(index, HG19_PATH, HG19_DBSNP_PATH, MAX_RANGE_RECORDS_IN_RESULT, am, qual, ad);
    }

    @GET
    @Path("/hg19/status")
    public Response getStatus19() {
        try{
            String status = QueryEngine.getRepoStatus(HG19_STATUS_PATH);
            return Response.status(OK).entity(status).build();
        }catch(Exception e){
            logger.error(e);
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @GET
    @Path("/chm13v2/{index}")
    public Response getResultCHM13V2(@PathParam("index") String index, @QueryParam("am") Double am, @QueryParam("qual") Integer qual, @QueryParam("ad") Integer ad) {
        return getResult(index, CHM13V2_PATH, CHM13V2_DBSNP_PATH, MAX_RANGE_RECORDS_IN_RESULT, am, qual, ad);
    }

    @GET
    @Path("/chm13v2/status")
    public Response getStatusCHM13V2() {
        try{
            String status = QueryEngine.getRepoStatus(CHM13V2_STATUS_PATH);
            return Response.status(OK).entity(status).build();
        }catch(Exception e){
            logger.error(e);
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    Response getResult(String index, String repoPath, String dbSnpPath, int maxRangeResult, Double am, Integer qual, Integer ad){

        logger.debug("Got request for index: " + index + " from path " + repoPath + ", am = " + am + ", qual = " + qual + ", ad =" + ad);

        String[] indexSplit = index.split(":");

        if (indexSplit.length != 2 && !index.contains("rs")){
            return Response.status(BAD_REQUEST).build();
        }else {

            String chrom;
            String fromCoordinate;
            String toCoordinate;

            // DBSNP flow
            if (index.contains("rs")){
                Pair<String, String> coordinate = dbsnpToCoordinate(dbSnpPath, index.replace("rs", ""));
                if (coordinate == null){
                    return Response.status(BAD_REQUEST).build();
                }
                chrom = coordinate.getLeft();
                fromCoordinate = coordinate.getRight();
                toCoordinate = fromCoordinate;
            }else {
                chrom = indexSplit[0].toLowerCase().trim();
                String[] posSplit = indexSplit[1].trim().split("-");
                if (posSplit.length != 2) {
                    return Response.status(BAD_REQUEST).build();
                }else{
                    fromCoordinate = posSplit[0].trim();
                    toCoordinate = posSplit[1].trim();
                }
            }

            if (VALID_CHROMOSOMES.contains(chrom)) {
                return handleRange(fromCoordinate, toCoordinate, chrom, repoPath, maxRangeResult, am, qual, ad) ;
            }else{
                return Response.status(BAD_REQUEST).build();
            }

        }

    }

    private static Response handleRange(String fromStr, String toStr, String chromosome, String repoPath, int maxRangeResult, Double am, Integer qual, Integer ad){
        int from;
        int to;
        try {
            from = Integer.parseInt(fromStr);
            to = Integer.parseInt(toStr);
        } catch (Exception e) {
            logger.error(e);
            return Response.status(BAD_REQUEST).build();
        }
        logger.debug("chrom = " + chromosome + ", from = " + from + ", to = " + to);

        try {
            String result = QueryEngine.getMutationsByRange(chromosome, from, to, repoPath, maxRangeResult, am, qual, ad);
            return Response.status(OK).entity(result).build();
        } catch (Exception e) {
            logger.error(e);
            if (e instanceof AnalysisException) {
                return Response.status(BAD_REQUEST).build();
            } else {
                return Response.status(INTERNAL_SERVER_ERROR).build();
            }
        }
    }

}
