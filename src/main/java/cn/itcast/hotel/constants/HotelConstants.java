package cn.itcast.hotel.constants;

/**
 * 作者： 王志博
 *
 * @version 1.0
 * @create 2023/11/13 19:41
 * @since 1.0
 */
public class HotelConstants {
    public static final String MAPPING_TEMPLATE = "{\n" +
            "  \"mappings\": {\n" +
            "    \"properties\": {\n" +
            "      \"id\":{\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"name\":{\n" +
            "        \"type\": \"text\",\n" +
            "        \"analyzer\": \"ik_max_word\",\n" +
            "        \"copy_to\": \"all\"\n" +
            "      },\n" +
            "      \"address\":{\n" +
            "        \"type\": \"keyword\",\n" +
            "        \"index\":false\n" +
            "      },\n" +
            "      \"price\":{\n" +
            "        \"type\":\"integer\"\n" +
            "      },\n" +
            "      \"score\":{\n" +
            "        \"type\":\"integer\"\n" +
            "      },\n" +
            "      \"brand\":{\n" +
            "        \"type\":\"keyword\",\n" +
            "        \"copy_to\": \"all\"\n" +
            "      },\n" +
            "      \"city\":{\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"starName\":{\n" +
            "        \"type\": \"keyword\"\n" +
            "      },\n" +
            "      \"bussiness\":{\n" +
            "        \"type\": \"keyword\",\n" +
            "        \"copy_to\": \"all\"\n" +
            "      },\n" +
            "      \"location\":{\n" +
            "        \"type\": \"geo_point\"\n" +
            "      },\n" +
            "      \"pic\":{\n" +
            "        \"type\": \"keyword\",\n" +
            "        \"index\": false\n" +
            "      },\n" +
            "      \"all\":{\n" +
            "        \"type\": \"text\",\n" +
            "        \"analyzer\": \"ik_max_word\", \n" +
            "        \"index\": true\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
}
