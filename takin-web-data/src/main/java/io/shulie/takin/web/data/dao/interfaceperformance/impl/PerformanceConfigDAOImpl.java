package io.shulie.takin.web.data.dao.interfaceperformance.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.shulie.takin.common.beans.page.PagingList;
import io.shulie.takin.web.common.vo.interfaceperformance.PerformanceConfigVO;
import io.shulie.takin.web.data.dao.interfaceperformance.PerformanceConfigDAO;
import io.shulie.takin.web.data.mapper.mysql.InterfacePerformanceConfigMapper;
import io.shulie.takin.web.data.model.mysql.InterfacePerformanceConfigEntity;
import io.shulie.takin.web.data.param.interfaceperformance.PerformanceConfigQueryParam;
import io.shulie.takin.web.data.util.MPUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author xingchen
 * @description: TODO
 * @date 2022/5/19 11:53 上午
 */
@Service
public class PerformanceConfigDAOImpl implements PerformanceConfigDAO,
        MPUtil<InterfacePerformanceConfigEntity> {
    @Resource
    private InterfacePerformanceConfigMapper interfacePerformanceConfigMapper;

    /**
     * 新增接口压测配置
     *
     * @param entity
     */
    @Override
    public void add(InterfacePerformanceConfigEntity entity) {
        interfacePerformanceConfigMapper.insert(entity);
    }

    @Override
    public InterfacePerformanceConfigEntity queryConfigByName(String name) {
        QueryWrapper<InterfacePerformanceConfigEntity> wrapper = new QueryWrapper<>();
        wrapper.eq("name", name);
        wrapper.eq("is_delete", 0);
        wrapper.last("limit 1");
        return interfacePerformanceConfigMapper.selectOne(wrapper);
    }

    /**
     * 分页查询数据
     *
     * @param param
     * @return
     */
    @Override
    public PagingList<PerformanceConfigVO> pageList(PerformanceConfigQueryParam param) {
        QueryWrapper<InterfacePerformanceConfigEntity> queryWrapper = this.getWrapper(param);
        Page<InterfacePerformanceConfigEntity> page = new Page<>(param.getCurrent() + 1, param.getPageSize());
        queryWrapper.orderByDesc("gmt_modified");
        IPage<InterfacePerformanceConfigEntity> pageList = interfacePerformanceConfigMapper.selectPage(page, queryWrapper);
        if (pageList.getRecords().isEmpty()) {
            return PagingList.empty();
        }
        List<PerformanceConfigVO> results = pageList.getRecords().stream().map(entity -> {
            PerformanceConfigVO result = new PerformanceConfigVO();
            BeanUtils.copyProperties(entity, result);
            return result;
        }).collect(Collectors.toList());
        return PagingList.of(results, pageList.getTotal());
    }

    @Override
    public void delete(Long id) {
        InterfacePerformanceConfigEntity updateEntity = new InterfacePerformanceConfigEntity();
        updateEntity.setId(id);
        updateEntity.setIsDeleted(true);
        updateEntity.setGmtModified(new Date());
        interfacePerformanceConfigMapper.updateById(updateEntity);
    }

    public QueryWrapper<InterfacePerformanceConfigEntity> getWrapper(PerformanceConfigQueryParam param) {
        QueryWrapper<InterfacePerformanceConfigEntity> queryWrapper = new QueryWrapper<>();
        if (param == null) {
            return queryWrapper;
        }
        // 模糊匹配
        if (StringUtils.isNotBlank(param.getQueryName())) {
            queryWrapper.like("name", param.getQueryName());
        }
        return queryWrapper;
    }
}