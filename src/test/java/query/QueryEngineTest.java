package query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static query.QueryEngine.getRepoStatus;
import static rest.MainTest.REPO_PATH_RANGES;
import static rest.MainTest.STATUS_PATH;

public class QueryEngineTest {

    static{
        Logger.getLogger("org.apache").setLevel(Level.WARN);
    }
    
    private static final ObjectMapper objectMapper = new ObjectMapper();

    //TODO utils for json parsing

    @Test
    public void getMutationsByRangeTest() throws IOException {

        // range of two files, count of one
        String result = QueryEngine.getMutationsByRange("2", 25234482, 25330557, REPO_PATH_RANGES, 9);
        Assert.assertNotNull(result);
        System.out.println(result);

        JsonNode jsonResult = objectMapper.readTree(result);
        Assert.assertEquals(10, jsonResult.get("count").asInt());

        ArrayNode dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(9, dataArray.size());

        JsonNode first = dataArray.get(0);
        Assert.assertEquals(25234482, first.get("pos").asInt());
        Assert.assertEquals("C", ((ArrayNode)first.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("T", ((ArrayNode)first.get("entries")).get(0).get("alt").asText());
        Assert.assertEquals("impact 1 test", ((ArrayNode)first.get("entries")).get(0).get("impact").asText());

        JsonNode last = dataArray.get(8);
        Assert.assertEquals(25247044, last.get("pos").asInt());
        Assert.assertEquals("C", ((ArrayNode)last.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("T", ((ArrayNode)last.get("entries")).get(0).get("alt").asText());

        // range of two
        result = QueryEngine.getMutationsByRange("2", 25234482, 25330557, REPO_PATH_RANGES, 100);
        Assert.assertNotNull(result);
        System.out.println(result);

        jsonResult = objectMapper.readTree(result);
        Assert.assertEquals(10, jsonResult.get("count").asInt());

        dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(10, dataArray.size());

        first = dataArray.get(0);
        Assert.assertEquals(25234482, first.get("pos").asInt());
        Assert.assertEquals("C", ((ArrayNode)first.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("T", ((ArrayNode)first.get("entries")).get(0).get("alt").asText());

        last = dataArray.get(9);
        Assert.assertEquals(25313958, last.get("pos").asInt());
        Assert.assertEquals("G", ((ArrayNode)last.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("A", ((ArrayNode)last.get("entries")).get(0).get("alt").asText());

        // range of one
        result = QueryEngine.getMutationsByRange("2", 25234482, 25234490, REPO_PATH_RANGES, 100);
        Assert.assertNotNull(result);
        System.out.println(result);

        jsonResult = objectMapper.readTree(result);
        Assert.assertEquals(1, jsonResult.get("count").asInt());
        dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(1, dataArray.size());

        // test counter counts mutations (rather than positions)
        result = QueryEngine.getMutationsByRange("2", 47805600, 47805603, REPO_PATH_RANGES, 100);
        Assert.assertNotNull(result);
        System.out.println(result);
        jsonResult = objectMapper.readTree(result);
        Assert.assertEquals(2, jsonResult.get("count").asInt());
        dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(1, dataArray.size());

        // empty result
        result = QueryEngine.getMutationsByRange("2", 25234483, 25234490, REPO_PATH_RANGES, 100);
        Assert.assertNotNull(result);
        System.out.println(result);

        jsonResult = objectMapper.readTree(result);
        Assert.assertEquals(0, jsonResult.get("count").asInt());
        dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(0, dataArray.size());

        //invalid range
        Assert.assertThrows(Exception.class,
                ()-> QueryEngine.getMutationsByRange("2", 500000000, 600000000, REPO_PATH_RANGES, 100));

        //invalid chromosome
        Assert.assertThrows(Exception.class,
                ()-> QueryEngine.getMutationsByRange("e", 1, 2, REPO_PATH_RANGES, 100));

        // test alpha
        result = QueryEngine.getMutationsByRange("1", 162778659, 162778659, REPO_PATH_RANGES, 100);
        Assert.assertNotNull(result);
        System.out.println(result);
        jsonResult = objectMapper.readTree(result);
        Assert.assertEquals(1, jsonResult.get("count").asInt());
        dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(1, dataArray.size());
        Assert.assertEquals(0.9942, ((ArrayNode)dataArray.get(0).get("entries")).get(0).get("alphamissense").asDouble(), 0.000001);
    }

    @Test
    public void getRepoStatusTest() throws IOException {
        String result = getRepoStatus(STATUS_PATH);
        Assert.assertNotNull(result);
        System.out.println(result);

        JsonNode jsonResult = objectMapper.readTree(result);
        Assert.assertEquals(1061550583, jsonResult.get("mutations_num").asInt());
        Assert.assertEquals(9997, jsonResult.get("samples_num").asInt());
        Assert.assertEquals("2021-09-08 06:32:20.432", jsonResult.get("update_date").asText());
    }
}
