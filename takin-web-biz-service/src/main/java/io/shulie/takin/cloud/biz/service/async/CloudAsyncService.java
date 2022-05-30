package io.shulie.takin.cloud.biz.service.async;

import io.shulie.takin.cloud.biz.collector.collector.AbstractIndicators.ResourceContext;
import io.shulie.takin.web.biz.checker.StartConditionCheckerContext;
import org.springframework.scheduling.annotation.Async;

/**
 * 异步服务
 *
 * @author qianshui
 * @date 2020/10/30 下午7:13
 */
public interface CloudAsyncService {

    void checkPodStartedTask(StartConditionCheckerContext context);

    void checkJmeterStartedTask(ResourceContext context);

    void checkJmeterHeartbeatTask(ResourceContext context);

    void checkPodHeartbeatTask(ResourceContext context);

    void checkStartTimeout(Long sceneId);

    void pressureStop(Long delay, String resourceId, Long jobId);
}
