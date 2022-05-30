package io.shulie.takin.web.data.dao.config.impl;

import java.util.Date;
import java.util.List;
import java.util.Objects;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.toolkit.SqlHelper;
import io.shulie.takin.web.common.constant.AppConstants;
import io.shulie.takin.web.common.enums.config.ConfigServerKeyEnum;
import io.shulie.takin.web.common.util.RedisHelper;
import io.shulie.takin.web.data.dao.config.ConfigServerDAO;
import io.shulie.takin.web.data.mapper.mysql.ConfigServerMapper;
import io.shulie.takin.web.data.model.mysql.ConfigServerEntity;
import io.shulie.takin.web.data.param.config.UpdateConfigServerParam;
import io.shulie.takin.web.data.result.config.ConfigServerDetailResult;
import io.shulie.takin.web.data.util.ConfigServerHelper;
import io.shulie.takin.web.data.util.MPUtil;
import io.shulie.takin.web.ext.util.WebPluginUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 配置表-服务的配置(ConfigServer)表数据库 dao 层实现
 *
 * @author liuchuan
 * @date 2021-10-12 11:17:20
 */
@Service
public class ConfigServerDAOImpl implements ConfigServerDAO, MPUtil<ConfigServerEntity>, AppConstants {

    @Autowired
    private ConfigServerMapper configServerMapper;

    @Override
    public String getTenantEnvValueByKey(String key) {
        ConfigServerDetailResult tenantEnvConfig = this.getTenantEnvConfigByKey(key);
        return tenantEnvConfig == null ? null : tenantEnvConfig.getValue();
    }

    @Override
    public String getGlobalNotTenantValueByKey(String key) {
        ConfigServerEntity configServer = configServerMapper.selectOne(this.getLimitOneLambdaQueryWrapper()
            .select(ConfigServerEntity::getValue).eq(ConfigServerEntity::getKey, key)
            .eq(ConfigServerEntity::getIsGlobal, YES).eq(ConfigServerEntity::getIsTenant, NO));
        return configServer == null ? "" : configServer.getValue();
    }

    @Override
    public String getTenantEnvValueByKeyAndTenantAppKeyAndEnvCode(String key, String tenantAppKey, String envCode) {
        List<ConfigServerDetailResult> valueList = configServerMapper.selectTenantEnvListByKey(key,
            WebPluginUtils.traceTenantAppKey(), WebPluginUtils.traceEnvCode());
        return valueList.isEmpty() ? null : valueList.get(0).getValue();
    }

    @Override
    public ConfigServerDetailResult getTenantEnvConfigByKey(String key) {
        List<ConfigServerDetailResult> valueList = configServerMapper.selectTenantEnvListByKey(key,
            WebPluginUtils.traceTenantAppKey(), WebPluginUtils.traceEnvCode());
        return valueList.isEmpty() ? null : valueList.get(0);
    }

    @Override
    public boolean updateGlobalValueByKey(UpdateConfigServerParam updateConfigServerParam) {
        return SqlHelper.retBool(configServerMapper.update(null, this.getLambdaUpdateWrapper()
            .eq(ConfigServerEntity::getKey, updateConfigServerParam.getKey())
            .eq(ConfigServerEntity::getIsTenant, AppConstants.NO)
            .set(ConfigServerEntity::getValue, updateConfigServerParam.getValue())));

    }

    @Override
    public String getUserConfigValueByKey(ConfigServerKeyEnum key) {
        ConfigServerEntity configServer = configServerMapper.selectOne(this.getLimitOneLambdaQueryWrapper()
            .select(ConfigServerEntity::getValue)
            .eq(ConfigServerEntity::getKey, key.getNow())
            .eq(ConfigServerEntity::getIsDeleted, NO)
            .eq(ConfigServerEntity::getUserId, WebPluginUtils.traceUserId())
            .eq(ConfigServerEntity::getIsTenant, key.getIsTenant()));
        return configServer == null ? key.getDefValue() : configServer.getValue();
    }

    @Override
    public void deleteUserEnvConfig(Long envRef) {
        if (Objects.nonNull(envRef)) {
            configServerMapper.delete(this.getTenantLambdaQueryWrapper()
                .eq(ConfigServerEntity::getKey, ConfigServerKeyEnum.TAKIN_TENANT_USER_DEFAULT_ENV.getNow())
                .eq(ConfigServerEntity::getValue, envRef));
        }
    }

    @Override
    public void saveOrUpdateEnvConfig(Long envRef) {
        if (Objects.nonNull(envRef)) {
            String envCode = String.valueOf(envRef);
            ConfigServerEntity configServer = configServerMapper.selectOne(getTenantLambdaQueryWrapper()
                .eq(ConfigServerEntity::getKey, ConfigServerKeyEnum.TAKIN_TENANT_USER_DEFAULT_ENV.getNow())
                .eq(ConfigServerEntity::getUserId, WebPluginUtils.traceUserId()));
            if (Objects.isNull(configServer)) {
                ConfigServerEntity entity = new ConfigServerEntity();
                entity.setKey(ConfigServerKeyEnum.TAKIN_TENANT_USER_DEFAULT_ENV.getNow());
                entity.setValue(envCode);
                entity.setTenantId(WebPluginUtils.traceTenantId());
                entity.setTenantAppKey(WebPluginUtils.traceTenantAppKey());
                entity.setEnvCode(WebPluginUtils.traceEnvCode());
                entity.setUserId(WebPluginUtils.traceUserId());
                entity.setIsGlobal(AppConstants.NO);
                entity.setIsTenant(AppConstants.NO);
                entity.setEdition(6);

                Date now = new Date();
                entity.setGmtCreate(now);
                entity.setGmtUpdate(now);
                configServerMapper.insert(entity);
            } else {
                configServerMapper.update(new ConfigServerEntity() {{setValue(envCode);}},
                    Wrappers.lambdaUpdate(ConfigServerEntity.class)
                        .eq(ConfigServerEntity::getKey, ConfigServerKeyEnum.TAKIN_TENANT_USER_DEFAULT_ENV.getNow())
                        .eq(ConfigServerEntity::getTenantId, WebPluginUtils.traceTenantId())
                        .eq(ConfigServerEntity::getUserId, WebPluginUtils.traceUserId())
                );
            }
            // 移除redis 缓存
            RedisHelper.hashDelete(ConfigServerHelper.getRedisKey(ConfigServerKeyEnum.TAKIN_TENANT_USER_DEFAULT_ENV)
                , ConfigServerHelper.getRedisFieldKey(ConfigServerKeyEnum.TAKIN_TENANT_USER_DEFAULT_ENV));
        }
    }
}

