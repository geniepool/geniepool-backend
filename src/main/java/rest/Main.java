package rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import query.QueryEngine;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static javax.ws.rs.core.Response.Status.*;


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

    private static final int MAX_RANGE_RECORDS_IN_RESULT;

    static{
        HG19_PATH = System.getProperty("REPO_HG_19_PATH");
        HG38_PATH = System.getProperty("REPO_HG_38_PATH");

        if (HG19_PATH == null || HG19_PATH.isEmpty() || HG38_PATH == null || HG38_PATH.isEmpty()){
            throw new IllegalStateException("repo path is empty!");
        }

        MAX_RANGE_RECORDS_IN_RESULT = Integer.parseInt(System.getProperty("MAX_RANGE_RECORDS_IN_RESULT", "100"));
    }

    @GET
    @Path("/hg38/{index}")
    public Response getResult38(@PathParam("index") String index) {
        return getResult(index, HG38_PATH, MAX_RANGE_RECORDS_IN_RESULT);
    }

    @GET
    @Path("/hg19/{index}")
    public Response getResult19(@PathParam("index") String index) {
        return getResult(index, HG19_PATH, MAX_RANGE_RECORDS_IN_RESULT);
    }

    Response getResult(String index, String repoPath, int maxRangeResult){

        logger.debug("Got request for index: " + index + " from path " + repoPath);

        String[] indexSplit = index.split(":");

        if (indexSplit.length != 2){
            return Response.status(BAD_REQUEST).build();
        }else {
            String chrom = indexSplit[0].toLowerCase().trim();

            if (VALID_CHROMOSOMES.contains(chrom)) {

                String[] posSplit = indexSplit[1].trim().split("-");

                if (posSplit.length == 1) {
                    return handleSingleCoordinate(chrom, posSplit[0].trim(), repoPath);
                }else if (posSplit.length == 2){
                    return handleRange(posSplit[0].trim(), posSplit[1].trim(), chrom, repoPath, maxRangeResult) ;
                }else{
                    return Response.status(BAD_REQUEST).build();
                }
            }else{
                return Response.status(BAD_REQUEST).build();
            }

        }

    }

    private static Response handleSingleCoordinate(String chromosome, String coordinate, String repoPath){
        int pos;
        try {
            pos = Integer.parseInt(coordinate);
        } catch (Exception e) {
            logger.error(e);
            return Response.status(BAD_REQUEST).build();
        }
        logger.debug("chrom = " + chromosome + ", pos = " + pos);

        try {
            String result = QueryEngine.getMutationsByIndex(chromosome, pos, repoPath);
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

    private static Response handleRange(String fromStr, String toStr, String chromosome, String repoPath, int maxRangeResult){
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
            String result = QueryEngine.getMutationsByRange(chromosome, from, to, repoPath, maxRangeResult);
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
