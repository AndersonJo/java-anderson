package ai.incredible.solr;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Objects;
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
        System.out.println(getClass());

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

    @Test
    void testSimpleQuerying() throws SolrServerException, IOException {
        // Input a document
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", "123456");
        document.addField("name", "Kenmore Dishwasher");
        document.addField("price", "599.99");
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
        }

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


}