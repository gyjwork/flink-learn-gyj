package com.gyj.Entites;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author gyj
 * @title: a
 * @projectName flink-learn-gyj
 * @description: TODO
 * @date 2019-05-1823:44
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MyEvent implements Serializable {
    private String id;
    private Long eventTime;
    private String info;
}


