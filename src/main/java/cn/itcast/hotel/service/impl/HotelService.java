package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;
    @Override
    public PageResult search(RequestParams params) {

        Integer page = params.getPage();
        Integer size = params.getSize();
        String location = params.getLocation();
        //1.request
        SearchRequest request = new SearchRequest("hotel");
        //2.DSL
        //2.1 query

        //关键字搜索
        buildBasicQuery(request,params);


        //2.2 分页
        request.source().from((page -1) * size).size(size);
        if(!StringUtils.isEmpty(location)){
            request.source().sort(SortBuilders
                    .geoDistanceSort("location",new GeoPoint(location))
                    .order(SortOrder.ASC)
                    .unit(DistanceUnit.KILOMETERS)
            );
        }

        //3.发送
        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return handleJson(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        SearchRequest request = new SearchRequest("hotel");
        //query
        buildBasicQuery(request,params);
        //
        request.source().size(0);
        //聚合
        buildAggregation(request);

        SearchResponse response = null;
        try {
            response = client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //System.out.println(response);
        Map<String,List<String>> result = new HashMap<>();
        Aggregations aggregations = response.getAggregations();
        //根据名称获取品牌结果
        List<String> brandList = getAggByName(aggregations,"brandAgg");
        result.put("brand",brandList);

        List<String> cityList = getAggByName(aggregations,"cityAgg");
        result.put("city",cityList);

        List<String> starList = getAggByName(aggregations,"starAgg");
        result.put("starName",starList);
        return result;
    }

    @Override
    public List<String> getSuggestion(String prefix) {
        //request
        try {
            SearchRequest request = new SearchRequest("hotel");
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions",
                    SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(prefix)
                            .skipDuplicates(true)
                            .size(10)
            ));
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //System.out.println(response);
            Suggest suggest = response.getSuggest();
            CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
            List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
            List<String> list = new ArrayList<>(options.size());
            for (CompletionSuggestion.Entry.Option option : options) {
                String text = option.getText().toString();
                list.add(text);
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> getAggByName(Aggregations aggregations,String aggName) {
        Terms brandTerms = (Terms) aggregations.get(aggName);
        //获取buckets
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        List<String> brandList = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            //System.out.println(key);
            brandList.add(key);
        }
        return brandList;
    }

    private static void buildAggregation(SearchRequest request) {
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(100));

        request.source().aggregation(AggregationBuilders
                .terms("cityAgg")
                .field("city")
                .size(100));

        request.source().aggregation(AggregationBuilders
                .terms("starAgg")
                .field("starName")
                .size(100));
    }

    private static void buildBasicQuery(SearchRequest request,RequestParams params) {

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        String key = params.getKey();

        if(StringUtils.isEmpty(key)){
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else{
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        //条件过滤
        if(!StringUtils.isEmpty(params.getCity())){
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }

        if(!StringUtils.isEmpty(params.getBrand())){
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }

        if(!StringUtils.isEmpty(params.getStartName())){
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStartName()));
        }
        if(params.getMaxPrice() != null && params.getMinPrice() != null){
//            System.out.println("最大值：" + params.getMaxPrice());
//            System.out.println("最小值：" + params.getMinPrice());
            if(params.getMaxPrice() < params.getMinPrice()){
                params.setMaxPrice(Integer.MAX_VALUE);
            }
            boolQuery.filter(QueryBuilders.rangeQuery("price")
                    .lte(params.getMaxPrice())
                    .gte(params.getMinPrice())
            );
        }
        //2.算分
        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        //原始查询
                        boolQuery,
                        //function score 的数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                //其中一个function score 元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                       QueryBuilders.termQuery("isAD",true),
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
        request.source().query(functionScoreQuery);
    }

    private static PageResult handleJson(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        long total = searchHits.getTotalHits().value;
//        System.out.println("搜索到" + total + "条数据");
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        for(SearchHit hit:hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            Object[] sortValues = hit.getSortValues();
            if(sortValues.length > 0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);

        }
        return new PageResult(total,hotels);
    }
}
