package com.heima.user.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.heima.model.common.dtos.ResponseResult;
import com.heima.model.user.pojos.dtos.LoginDto;
import com.heima.model.user.pojos.ApUser;
import org.springframework.stereotype.Service;

@Service
public interface ApUserService extends IService<ApUser> {
    public ResponseResult login(LoginDto dto);
}
