package query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static query.QueryEngine.EMPTY_RESULT;
import static rest.MainTest.REPO_PATH_RANGES;

public class QueryEngineTest {

    static{
        Logger.getLogger("org.apache").setLevel(Level.WARN);
    }
    
    private static final ObjectMapper objectMapper = new ObjectMapper();

    //TODO utils for json parsing

    @Test
    public void getMutationsByIndexTest() throws IOException {

        String result = QueryEngine.getMutationsByIndex("2", 25247044, REPO_PATH_RANGES);
        Assert.assertNotNull(result);
        System.out.println(result);
        JsonNode entries = ((ArrayNode) objectMapper.readTree(result).get("entries")).get(0);
        Assert.assertEquals("C", entries.get("ref").asText());
        Assert.assertEquals("T", entries.get("alt").asText());
        ArrayNode homArray = (ArrayNode) entries.get("hom");
        Assert.assertEquals(0, homArray.size());
        ArrayNode hetArray = (ArrayNode) entries.get("het");
        Assert.assertEquals(2, hetArray.size());

        result = QueryEngine.getMutationsByIndex("2", 26501857, REPO_PATH_RANGES);
        Assert.assertNotNull(result);
        System.out.println(result);
        entries = ((ArrayNode) objectMapper.readTree(result).get("entries")).get(0);
        Assert.assertEquals("T", entries.get("ref").asText());
        Assert.assertEquals("G", entries.get("alt").asText());
        homArray = (ArrayNode) entries.get("hom");
        Assert.assertEquals(2, homArray.size());
        hetArray = (ArrayNode) entries.get("het");
        Assert.assertEquals(0, hetArray.size());

        // empty result
        result = QueryEngine.getMutationsByIndex("2", 26501858, REPO_PATH_RANGES);
        Assert.assertNotNull(result);
        System.out.println(result);
        Assert.assertEquals(EMPTY_RESULT, result);

        // invalid index
        Assert.assertThrows(Exception.class,
                () -> QueryEngine.getMutationsByIndex("2", 500000000, REPO_PATH_RANGES));

        // invalid chromosome
        Assert.assertThrows(Exception.class,
                () -> QueryEngine.getMutationsByIndex("e", 1, REPO_PATH_RANGES));

    }


    @Test
    public void getMutationsByRangeTest() throws IOException {

        // range of two files, count of one
        String result = QueryEngine.getMutationsByRange("2", 25234482, 26501857, REPO_PATH_RANGES, 10);
        Assert.assertNotNull(result);
        System.out.println(result);

        JsonNode jsonResult = objectMapper.readTree(result);
        Assert.assertEquals(11, jsonResult.get("count").asInt());

        ArrayNode dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(10, dataArray.size());

        JsonNode first = dataArray.get(0);
        Assert.assertEquals(25234482, first.get("pos").asInt());
        Assert.assertEquals("C", ((ArrayNode)first.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("T", ((ArrayNode)first.get("entries")).get(0).get("alt").asText());

        JsonNode last = dataArray.get(9);
        Assert.assertEquals(25313958, last.get("pos").asInt());
        Assert.assertEquals("G", ((ArrayNode)last.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("A", ((ArrayNode)last.get("entries")).get(0).get("alt").asText());

        // range of two
        result = QueryEngine.getMutationsByRange("2", 25234482, 26501859, REPO_PATH_RANGES, 100);
        Assert.assertNotNull(result);
        System.out.println(result);

        jsonResult = objectMapper.readTree(result);
        Assert.assertEquals(11, jsonResult.get("count").asInt());

        dataArray = (ArrayNode)jsonResult.get("data");
        Assert.assertEquals(11, dataArray.size());

        first = dataArray.get(0);
        Assert.assertEquals(25234482, first.get("pos").asInt());
        Assert.assertEquals("C", ((ArrayNode)first.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("T", ((ArrayNode)first.get("entries")).get(0).get("alt").asText());

        last = dataArray.get(10);
        Assert.assertEquals(26501857, last.get("pos").asInt());
        Assert.assertEquals("T", ((ArrayNode)last.get("entries")).get(0).get("ref").asText());
        Assert.assertEquals("G", ((ArrayNode)last.get("entries")).get(0).get("alt").asText());

        // range of one
        result = QueryEngine.getMutationsByRange("2", 25234482, 25234490, REPO_PATH_RANGES, 100);
        Assert.assertNotNull(result);
        System.out.println(result);

        jsonResult = objectMapper.readTree(result);
        Assert.assertEquals(1, jsonResult.get("count").asInt());
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
    }

}
