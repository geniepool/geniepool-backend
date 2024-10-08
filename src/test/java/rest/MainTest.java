package rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.OK;

public class MainTest {

    static{
        Logger.getLogger("org.apache").setLevel(Level.WARN);
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    //TODO add more tests + test on Tomcat (use docker?)

    public static final String REPO_PATH_RANGES = "src/test/resources/repo_ranges/";
    public static final String REPO_PATH_RANGES_T2T = "src/test/resources/t2t/";
    public static final String STATUS_PATH = "src/test/resources/status/";
    public static final String DBSNP_PATH = "src/test/resources/dbsnp_index_hg38/";

    @Test
    public void getResultTest() throws IOException {

        System.setProperty("REPO_HG_19_PATH", REPO_PATH_RANGES);
        System.setProperty("REPO_HG_38_PATH", REPO_PATH_RANGES);
        System.setProperty("REPO_CHM13V2_PATH", REPO_PATH_RANGES_T2T);

        System.setProperty("HG19_STATUS_PATH", STATUS_PATH);
        System.setProperty("HG38_STATUS_PATH", STATUS_PATH);
        System.setProperty("CHM13V2_STATUS_PATH", STATUS_PATH);

        System.setProperty("HG19_DBSNP_PATH", DBSNP_PATH);
        System.setProperty("HG38_DBSNP_PATH", DBSNP_PATH);
        System.setProperty("CHM13V2_DBSNP_PATH", DBSNP_PATH);

        System.setProperty("MAX_RANGE_RECORDS_IN_RESULT", "10");

        // test common valid flow
        Response response = new Main().getResult19("X:77633124-77633124", null, null, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertNotNull(response.getEntity());
        JsonNode result = ((ArrayNode) objectMapper.readTree((String)response.getEntity()).get("data")).get(0);
        result = ((ArrayNode)result.get("entries")).get(0);
        Assert.assertEquals("G", result.get("ref").asText());
        Assert.assertEquals("A", result.get("alt").asText());
        ArrayNode homArray = (ArrayNode) result.get("hom");
        Assert.assertEquals(1, homArray.size());
        Assert.assertEquals("SRR14860530", homArray.get(0).get("id").asText());
        Assert.assertEquals("3,37", homArray.get(0).get("ad").asText());
        Assert.assertEquals(1376.31, homArray.get(0).get("qual").asDouble(), 0);
        ArrayNode hetArray = (ArrayNode) result.get("het");
        Assert.assertEquals(1, hetArray.size());
        Assert.assertEquals("SRR14860527", hetArray.get(0).get("id").asText());
        Assert.assertEquals(464.64, hetArray.get(0).get("qual").asDouble(), 0);
        Assert.assertEquals("2,13", hetArray.get(0).get("ad").asText());

        // test lower case
        response = new Main().getResult("x:77633124-77633124", REPO_PATH_RANGES, DBSNP_PATH, 10, null, null, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertNotNull(response.getEntity());
        result = ((ArrayNode) objectMapper.readTree((String)response.getEntity()).get("data")).get(0);
        result = ((ArrayNode)result.get("entries")).get(0);
        Assert.assertEquals("G", result.get("ref").asText());
        Assert.assertEquals("A", result.get("alt").asText());
        homArray = (ArrayNode) result.get("hom");
        Assert.assertEquals(1, homArray.size());
        Assert.assertEquals("SRR14860530", homArray.get(0).get("id").asText());
        hetArray = (ArrayNode) result.get("het");
        Assert.assertEquals(1, hetArray.size());
        Assert.assertEquals("SRR14860527", hetArray.get(0).get("id").asText());

        //test range query
        response = new Main().getResult("2:25234482-25330557", REPO_PATH_RANGES, DBSNP_PATH, 9, null, null, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());

        JsonNode jsonResult = objectMapper.readTree((String)response.getEntity());
        Assert.assertEquals(10, jsonResult.get("count").asInt());

        ArrayNode dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(9, dataArray.size());

        JsonNode first = dataArray.get(0);
        Assert.assertEquals("2", first.get("chrom").asText());
        Assert.assertEquals(25234482, first.get("pos").asInt());
        Assert.assertEquals("C", ((ArrayNode)first.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("T", ((ArrayNode)first.get("entries")).get(0).get("alt").asText());
        Assert.assertEquals("impact 1 test", ((ArrayNode)first.get("entries")).get(0).get("impact").asText());
        Assert.assertEquals("rs123123", ((ArrayNode)first.get("entries")).get(0).get("dbSNP").asText());

        JsonNode last = dataArray.get(8);
        Assert.assertEquals(25247044, last.get("pos").asInt());
        Assert.assertEquals("C", ((ArrayNode)last.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("T", ((ArrayNode)last.get("entries")).get(0).get("alt").asText());

        // test empty case
        response = new Main().getResult("x:15800112-15800112", REPO_PATH_RANGES, DBSNP_PATH, 10, null, null, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertNotNull(response.getEntity());
        Assert.assertEquals(0, objectMapper.readTree((String)response.getEntity()).get("count").asInt());

        // test bad input 1
        response = new Main().getResult("adkwjfh", REPO_PATH_RANGES, DBSNP_PATH, 10, null, null, null);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad input 2
        response = new Main().getResult("s:sss", REPO_PATH_RANGES, DBSNP_PATH, 10, null, null, null);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad input 3
        response = new Main().getResult("s:12345", REPO_PATH_RANGES, DBSNP_PATH, 10, null, null, null);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad input 4
        response = new Main().getResult("x:500000000", REPO_PATH_RANGES, DBSNP_PATH, 10, null, null, null);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test alpha
        response = new Main().getResult38("1:162778659-162778659", null, null, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());

        // test t2t
        response = new Main().getResultCHM13V2("1:722494-722594", null, null, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());

        // test alpha filtering
        response = new Main().getResult38("1:162777659-162779659", 0.5, null, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertEquals(1, objectMapper.readTree((String)response.getEntity()).get("count").asInt());

        response = new Main().getResult38("1:162700001-162799999", null, null, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertEquals(8, objectMapper.readTree((String)response.getEntity()).get("count").asInt());

        response = new Main().getResult38("1:162700001-162799999", 0.9955, null, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertEquals(0, objectMapper.readTree((String)response.getEntity()).get("count").asInt());

        // test ad and qual
        response = new Main().getResultCHM13V2("1:722494-722594", null, 70, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertEquals(0, objectMapper.readTree((String)response.getEntity()).get("count").asInt());

        response = new Main().getResultCHM13V2("1:722494-722594", null, 65, null);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertEquals(1, objectMapper.readTree((String)response.getEntity()).get("count").asInt());

        response = new Main().getResultCHM13V2("1:722494-722594", null, 65, 4);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertEquals(0, objectMapper.readTree((String)response.getEntity()).get("count").asInt());

        response = new Main().getResultCHM13V2("1:722494-722594", null, 65, 3);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertEquals(1, objectMapper.readTree((String)response.getEntity()).get("count").asInt());

        // test query by dbSNP (pos_bucket=1154 was taken from production)
        Response response1 = new Main().getResult38("rs524965", null, null, null);
        Assert.assertEquals(OK.getStatusCode(), response1.getStatus());
        Response response2 = new Main().getResult38("1:115480755-115480755", null, null, null);
        Assert.assertEquals(OK.getStatusCode(), response2.getStatus());
        Assert.assertEquals(response1.getEntity(), response2.getEntity());
        System.out.println(response2.getEntity());

        response = new Main().getResult38("rs2131238", null, null, null);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());


        // test not-existing number
        response = new Main().getResult38("rs24050604", null, null, null);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad number
        response = new Main().getResult38("rs32483048230948", null, null, null);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test no number
        response = new Main().getResult38("rsskdfjsdhf", null, null, null);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

    }

    @Test
    public void getStatusTest() throws IOException {
        System.setProperty("REPO_HG_19_PATH", REPO_PATH_RANGES);
        System.setProperty("REPO_HG_38_PATH", REPO_PATH_RANGES);
        System.setProperty("HG19_STATUS_PATH", STATUS_PATH);
        System.setProperty("HG38_STATUS_PATH", STATUS_PATH);

        Response response = new Main().getStatus19();
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        JsonNode result = objectMapper.readTree(response.getEntity().toString());
        System.out.println(result);
        Assert.assertNotNull(result);

        response = new Main().getStatus38();
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
    }

}
