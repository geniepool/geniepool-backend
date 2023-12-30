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
    public static final String STATUS_PATH = "src/test/resources/status/";

    @Test
    public void getResultTest() throws IOException {

        System.setProperty("REPO_HG_19_PATH", REPO_PATH_RANGES);
        System.setProperty("REPO_HG_38_PATH", REPO_PATH_RANGES);
        System.setProperty("HG19_STATUS_PATH", STATUS_PATH);
        System.setProperty("HG38_STATUS_PATH", STATUS_PATH);
        System.setProperty("MAX_RANGE_RECORDS_IN_RESULT", "10");

        // test common valid flow
        Response response = new Main().getResult19("X:77633124-77633124");
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
        response = new Main().getResult("x:77633124-77633124", REPO_PATH_RANGES, 10);
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
        response = new Main().getResult("2:25234482-25330557", REPO_PATH_RANGES, 9);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());

        JsonNode jsonResult = objectMapper.readTree((String)response.getEntity());
        Assert.assertEquals(10, jsonResult.get("count").asInt());

        ArrayNode dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(9, dataArray.size());

        JsonNode first = dataArray.get(0);
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
        response = new Main().getResult("x:15800112-15800112", REPO_PATH_RANGES, 10);
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
        Assert.assertNotNull(response.getEntity());
        Assert.assertEquals(0, objectMapper.readTree((String)response.getEntity()).get("count").asInt());

        // test bad input 1
        response = new Main().getResult("adkwjfh", REPO_PATH_RANGES, 10);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad input 2
        response = new Main().getResult("s:sss", REPO_PATH_RANGES, 10);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad input 3
        response = new Main().getResult("s:12345", REPO_PATH_RANGES, 10);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test bad input 4
        response = new Main().getResult("x:500000000", REPO_PATH_RANGES, 10);
        Assert.assertEquals(BAD_REQUEST.getStatusCode(), response.getStatus());

        // test alpha
        response = new Main().getResult38("1:162778659-162778659");
        Assert.assertEquals(OK.getStatusCode(), response.getStatus());
        System.out.println(response.getEntity());
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
