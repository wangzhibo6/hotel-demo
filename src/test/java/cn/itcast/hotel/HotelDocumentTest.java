package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

/**
 * 作者： 王志博
 *
 * @version 1.0
 * @create 2023/11/13 16:43
 * @since 1.0
 */
@SpringBootTest
public class HotelDocumentTest {

    @Autowired
    private IHotelService hotelService;

    @Test
    void testInit(){
        System.out.println(client);
    }

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
    void testAddDocument() throws IOException {
        //根据id查询
        Hotel hotel = hotelService.getById(61083L);
        HotelDoc hotelDoc = new HotelDoc(hotel);
        //1. request对象
        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
        //json文档
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        //请求
        client.index(request,RequestOptions.DEFAULT);
    }

    @Test
    void testGetDocument() throws IOException {
        //new GetRequest("hotel","61083")
        GetRequest request = new GetRequest("hotel").id("61083");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        String json = response.getSourceAsString();
        System.out.println(json);
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println("++++++++++++");
        System.out.println(hotelDoc);
    }

    @Test
    void testUpdateDocument() throws IOException{
        //1.准备Request
        UpdateRequest request = new UpdateRequest("hotel","61083");
        //2.参数
        request.doc(
                "price","952",
                "starName","四钻"
        );
        //3.发送请求
        client.update(request,RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDocument() throws IOException{
        DeleteRequest request = new DeleteRequest("hotel","61083");
        client.delete(request,RequestOptions.DEFAULT);
    }
    //批处理
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest request = new BulkRequest();
        //批量查询数据
        List<Hotel> hotels = hotelService.list();
        //转换为hoteldoc
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel")
                    .id(hotel.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        //准备参数
        client.bulk(request,RequestOptions.DEFAULT);
    }

}
