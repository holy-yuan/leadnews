package com.heima.model.user.pojos.dtos;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class LoginDto {
    @ApiModelProperty(value = "手机号",required = true)
    private String phone;

    @ApiModelProperty(value = "密码",required = true)
    private String password;
}
