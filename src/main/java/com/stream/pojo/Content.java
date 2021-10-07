package com.stream.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Content {
    private String img;
    private String price;
    private String title;
    private String commit;
    private String shopnum;
}
