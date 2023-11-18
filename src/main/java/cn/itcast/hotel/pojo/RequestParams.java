package cn.itcast.hotel.pojo;

import lombok.Data;

/**
 * 作者： 王志博
 *
 * @version 1.0
 * @create 2023/11/16 11:28
 * @since 1.0
 */
@Data
public class RequestParams {
    private String key;
    private Integer page;
    private Integer size;
    private String sortBy;
    private String city;
    private String brand;
    private String startName;
    private Integer minPrice;
    private Integer maxPrice;
    private String location;
}
