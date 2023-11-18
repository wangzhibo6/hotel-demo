package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 作者： 王志博
 *
 * @version 1.0
 * @create 2023/11/13 16:43
 * @since 1.0
 */
@SpringBootTest
public class HotelSearchTest {

    @Autowired
    private IHotelService hotelService;

    private RestHighLevelClient client;
    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.88.189:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException{
        this.client.close();
    }

    @Test
    void testMatchAll() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response);
        //解析
        handleJson(response);
    }

    @Test
    void testMatch() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //解析
        handleJson(response);
    }

    @Test
    void testBool() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        //boolquery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //term
        boolQuery.must(QueryBuilders.termQuery("city","上海"));
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(200));
        request.source().query(boolQuery);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //解析
        handleJson(response);
    }

    @Test
    void testPageAndSort() throws IOException {
        int page =2,size =5;
        SearchRequest request = new SearchRequest("hotel");
        //query
        request.source().query(QueryBuilders.matchAllQuery());
        //sort
        request.source().sort("price", SortOrder.ASC);
        //from.size
        request.source().from((page -1) * size).size(size);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //解析
        handleJson(response);
    }

    @Test
    void testHighLight() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        request.source().highlighter(new HighlightBuilder()
                .field("name")
                .requireFieldMatch(false));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //解析
        handleJson(response);
    }

    @Test
    void testSuggest() throws IOException {
        //request
        SearchRequest request = new SearchRequest("hotel");
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions",
                SuggestBuilders.completionSuggestion("suggestion")
                        .prefix("h")
                        .skipDuplicates(true)
                        .size(10)
        ));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
//        System.out.println(response);
        Suggest suggest = response.getSuggest();
        CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
        List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
        for (CompletionSuggestion.Entry.Option option : options) {
            String text = option.getText().toString();
            System.out.println(text);
        }

    }

    @Test
    void testAggregation() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().size(0);
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(10));

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response);
        Aggregations aggregations = response.getAggregations();
        Terms brandTerms = (Terms) aggregations.get("brandAgg");
        //获取buckets
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            System.out.println(key);
        }
    }

    private static void handleJson(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        long total = searchHits.getTotalHits().value;
        System.out.println("搜索到" + total + "条数据");
        SearchHit[] hits = searchHits.getHits();
        for(SearchHit hit:hits){
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //获取高亮
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(!CollectionUtils.isEmpty(highlightFields)){
                HighlightField highlightField = highlightFields.get("name");
                if(highlightField != null){
                    //获取高亮值
                    String name = highlightField.getFragments()[0].string();
                    //覆盖非高亮
                    hotelDoc.setName(name);
                }
            }
            //根据字段名获取高亮结果
            System.out.println(hotelDoc);
        }
    }

}
