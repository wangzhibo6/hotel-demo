package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

/**
 * 作者： 王志博
 *
 * @version 1.0
 * @create 2023/11/13 16:43
 * @since 1.0
 */
public class HotelIndexTest {

    @Test
    void testInit(){
        System.out.println(client);
    }

    private RestHighLevelClient client;
    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.88.187:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException{
        this.client.close();
    }

    @Test
    void createHotelIndex() throws IOException {
        //1. 创建request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //2. 请求参数：DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        //3.发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteHotelIndex() throws IOException {
        //deleteRequest对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        //2.delete
        client.indices().delete(request,RequestOptions.DEFAULT);
    }

    @Test
    void testExistsHotelIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("hotel");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("hotel索引库：" + (exists?"存在":"不存在"));
    }
}
