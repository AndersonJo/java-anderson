package ai.incredible.solr;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

/**
 * 시작전에 다음을 실행.
 * ./bin/solr create -c users
 * ./bin/solr start
 *
 * 삭제는..
 * rm -rf server/solr/users/
 * 그리고 재시작
 */
class SolrTest {

    private static final String url = "http://localhost:8983/solr/users";

    private final HttpSolrClient client;

    public SolrTest() throws SolrServerException, IOException {
        client = new HttpSolrClient.Builder(url).build();
        client.setParser(new XMLResponseParser());
    }

    User[] getGsonData() {
        try (Reader reader = new InputStreamReader(Objects.requireNonNull(getClass()
                .getClassLoader()
                .getResourceAsStream("data.json")))) {

            User[] data = new Gson().fromJson(reader, User[].class);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    Location[] getLocationData() {
        try (Reader reader = new InputStreamReader(Objects.requireNonNull(getClass()
                .getClassLoader()
                .getResourceAsStream("geodata.json")))) {

            Location[] data = new Gson().fromJson(reader, Location[].class);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    @Test
    void testSimpleQuerying() throws SolrServerException, IOException {
        // Input a document
        ArrayList<Long> bookIds = new ArrayList<Long>(Arrays.asList(1L, 2L, 3L, 4L, 5L));

        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", "123456");
        document.addField("name", "Kenmore Dishwasher");
        document.addField("price", "599.99");
        document.addField("bookIds", bookIds);
        document.addField("cum", 10L);
        document.addField("cum", 20L);
        document.addField("cum", 30L);
        document.addField("cum", 40L);
        document.addField("cum", 50L);
        document.setField("status", "Good");
        document.setField("status", "Bad");
        document.setField("status", "AWESOME!");
        client.add(document);
        client.commit();

        // Get Response from Solr
        SolrQuery query = new SolrQuery();
        query.set("q", "price:599.99");
        QueryResponse response = client.query(query);

        // Validation
        SolrDocumentList docs = response.getResults();
        assertEquals(1, docs.size());
        for (SolrDocument doc : response.getResults()) {
            assertEquals("Kenmore Dishwasher", doc.getFieldValue("name"));
            assertEquals((Double) 599.99, (Double) doc.getFieldValue("price"));

            // 처리 방식이 다르다. Collection<String> 으로 되어 있음
            ArrayList<Long> values = doc.getFieldValues("bookIds")
                    .stream()
                    .map(c -> (Long) c)
                    .collect(Collectors.toCollection(ArrayList::new));
            for (int i = 0; i < values.size(); i++) {
                assertEquals(i + 1, values.get(i));
            }

            // Collection<Long> 으로 되어 있음.
            ArrayList<Long> values2 = doc.getFieldValues("cum")
                    .stream()
                    .map(c -> (Long) c)
                    .collect(Collectors.toCollection(ArrayList::new));
            for (int i = 0; i < values.size(); i++) {
                assertEquals((i + 1) * 10L, values2.get(i));
            }

            // Test setField for duplicate array.
            assertEquals(1, doc.getFieldValues("status").size());

        }

        // Get Book ID
        query = new SolrQuery();
        query.set("q", "bookIds:3");
        response = client.query(query);
        assertEquals(1, response.getResults().size());
        assertEquals(599.99, response.getResults().get(0).getFieldValue("price"));

        // Get not-existing book ID
        query = new SolrQuery();
        query.set("q", "bookIds:100");
        response = client.query(query);
        assertEquals(0, response.getResults().size());

        // Get cum
        query = new SolrQuery();
        query.set("q", "cum:(20 100)");
        response = client.query(query);
        assertEquals(1, response.getResults().size());
        assertEquals(599.99, response.getResults().get(0).getFieldValue("price"));

        // Get non-existing cum
        query = new SolrQuery();
        query.set("q", "cum:(500 100)");
        response = client.query(query);
        assertEquals(0, response.getResults().size());


        client.deleteByQuery("*:*");
        client.commit();
    }

    @Test
    void testGSonSolrQuerying() throws SolrServerException, IOException {
        User[] users = getGsonData();
        for (User user : users) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", user.getUserId());
            doc.addField("name", user.getName());
            doc.addField("tags", user.getTags());
            client.add(doc);
            client.commit();
        }

        // Get Response from Solr
        SolrQuery query = new SolrQuery();
        query.set("q", "tags:Statistics");
        QueryResponse response = client.query(query);

        // Validation
        SolrDocumentList docs = response.getResults();
        assertEquals(2, docs.size());

        // Delete
        client.deleteByQuery("*:*");
        client.commit();
    }

    @Test
    void testAddFieldVSSetField() throws SolrServerException, IOException {
        SolrInputDocument document = new SolrInputDocument();
        document.setField("id", "987654321");
        document.setField("status1", "Good");
        document.setField("status1", "Bad");
        document.setField("status1", "AWESOME!");
        document.addField("status2", "Good");
        document.addField("status2", "Bad");
        document.addField("status2", "AWESOME!");
        client.add(document);
        client.commit();

        SolrQuery query = new SolrQuery();
        query.set("q", "id:987654321");
        QueryResponse response = client.query(query);
        SolrDocument responseDocument = response.getResults().get(0);

//        System.out.println(responseDocument.getFieldValue("status1"));
//        System.out.println(responseDocument.getFieldValue("status2"));
//
//        System.out.println(responseDocument.getFieldValues("status1"));
//        System.out.println(responseDocument.getFieldValues("status2"));

        ArrayList<String> status1 = responseDocument.getFieldValues("status1")
                .stream().map(v -> (String) v)
                .collect(Collectors.toCollection(ArrayList::new));

        ArrayList<String> status2 = responseDocument.getFieldValues("status2")
                .stream().map(v -> (String) v)
                .collect(Collectors.toCollection(ArrayList::new));

        assertEquals(1, status1.size());
        assertEquals(3, status2.size());

        // Delete
        client.deleteByQuery("*:*");
        client.commit();
    }

    @Test
    void testGeoFilter() throws SolrServerException, IOException {
        Location[] data = getLocationData();
        for(Location loc : data){
            SolrInputDocument document = new SolrInputDocument();
            document.setField("name", loc.getName());
            document.setField("lat", loc.getLat());
            document.setField("lng", loc.getLng());
            client.add(document);
        }
        client.commit();
        SolrQuery query = new SolrQuery();
        query.set("q", "{!geofilt d=10}&pt=37.499402,127.054207");
        QueryResponse response = client.query(query);

        for(SolrDocument doc:response.getResults()){
            System.out.println(doc.getFieldValue("name"));
        }




    }


    public static double distance(double lat1, double lng1, double lat2,
                                  double lng2, double el1, double el2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lng2 - lng1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters

        double height = el1 - el2;

        distance = Math.pow(distance, 2) + Math.pow(height, 2);

        return Math.sqrt(distance);
    }


}
