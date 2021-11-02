package io.shulie.takin.web.biz.service.linkmanage.impl;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import com.alibaba.fastjson.JSON;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pamirs.takin.common.util.DateUtils;
import com.pamirs.takin.entity.dao.linkguard.TLinkGuardMapper;
import com.pamirs.takin.entity.dao.linkmanage.TBusinessLinkManageTableMapper;
import com.pamirs.takin.entity.dao.linkmanage.TLinkManageTableMapper;
import com.pamirs.takin.entity.dao.linkmanage.TMiddlewareInfoMapper;
import com.pamirs.takin.entity.dao.linkmanage.TMiddlewareLinkRelateMapper;
import com.pamirs.takin.entity.dao.linkmanage.TSceneLinkRelateMapper;
import com.pamirs.takin.entity.dao.linkmanage.TSceneMapper;
import com.pamirs.takin.entity.domain.dto.EntranceSimpleDto;
import com.pamirs.takin.entity.domain.dto.linkmanage.*;
import com.pamirs.takin.entity.domain.dto.linkmanage.linkstatistics.ApplicationRemoteDto;
import com.pamirs.takin.entity.domain.dto.linkmanage.linkstatistics.BusinessCoverDto;
import com.pamirs.takin.entity.domain.dto.linkmanage.linkstatistics.LinkHistoryInfoDto;
import com.pamirs.takin.entity.domain.dto.linkmanage.linkstatistics.LinkRemarkDto;
import com.pamirs.takin.entity.domain.dto.linkmanage.linkstatistics.LinkRemarkmiddleWareDto;
import com.pamirs.takin.entity.domain.dto.linkmanage.linkstatistics.SystemProcessDto;
import com.pamirs.takin.entity.domain.dto.linkmanage.mapping.LinkDomainEnumMapping;
import com.pamirs.takin.entity.domain.dto.linkmanage.mapping.enums.LinkDomainEnum;
import com.pamirs.takin.entity.domain.dto.linkmanage.mapping.enums.MiddlewareTypeEnum;
import com.pamirs.takin.entity.domain.dto.linkmanage.mapping.enums.NodeClassEnum;
import com.pamirs.takin.entity.domain.entity.linkmanage.BusinessLinkManageTable;
import com.pamirs.takin.entity.domain.entity.linkmanage.LinkManageTable;
import com.pamirs.takin.entity.domain.entity.linkmanage.LinkQueryVo;
import com.pamirs.takin.entity.domain.entity.linkmanage.Scene;
import com.pamirs.takin.entity.domain.entity.linkmanage.SceneLinkRelate;
import com.pamirs.takin.entity.domain.entity.linkmanage.TMiddlewareInfo;
import com.pamirs.takin.entity.domain.entity.linkmanage.statistics.StatisticsQueryVo;
import com.pamirs.takin.entity.domain.entity.linkmanage.structure.Category;
import com.pamirs.takin.entity.domain.vo.linkmanage.BusinessFlowTree;
import com.pamirs.takin.entity.domain.vo.linkmanage.BusinessFlowVo;
import com.pamirs.takin.entity.domain.vo.linkmanage.MiddleWareEntity;
import com.pamirs.takin.entity.domain.vo.linkmanage.queryparam.BusinessQueryVo;
import com.pamirs.takin.entity.domain.vo.linkmanage.queryparam.SceneQueryVo;
import io.shulie.takin.cloud.common.utils.JmxUtil;
import io.shulie.takin.cloud.open.req.filemanager.FileCreateByStringParamReq;
import io.shulie.takin.cloud.open.req.scenemanage.ScriptAnalyzeRequest;
import io.shulie.takin.common.beans.response.ResponseResult;
import io.shulie.takin.ext.content.emus.NodeTypeEnum;
import io.shulie.takin.ext.content.script.ScriptNode;
import io.shulie.takin.utils.json.JsonHelper;
import io.shulie.takin.web.biz.cache.DictionaryCache;
import io.shulie.takin.web.biz.constant.BizOpConstants;
import io.shulie.takin.web.biz.convert.linkmanage.LinkManageConvert;
import io.shulie.takin.web.biz.pojo.request.filemanage.FileManageUpdateRequest;
import io.shulie.takin.web.biz.pojo.request.linkmanage.BusinessFlowDataFileRequest;
import io.shulie.takin.web.biz.pojo.request.linkmanage.BusinessFlowParseRequest;
import io.shulie.takin.web.biz.pojo.request.scriptmanage.ScriptManageDeployCreateRequest;
import io.shulie.takin.web.biz.pojo.request.scriptmanage.ScriptManageDeployUpdateRequest;
import io.shulie.takin.web.biz.pojo.response.application.AgentPluginSupportResponse;
import io.shulie.takin.web.biz.pojo.response.filemanage.FileManageResponse;
import io.shulie.takin.web.biz.pojo.response.linkmanage.*;
import io.shulie.takin.web.biz.pojo.response.scriptmanage.ScriptManageDeployDetailResponse;
import io.shulie.takin.web.biz.service.agent.AgentPluginSupportService;
import io.shulie.takin.web.biz.service.linkmanage.LinkManageService;
import io.shulie.takin.web.biz.service.scene.SceneService;
import io.shulie.takin.web.biz.service.scriptmanage.ScriptManageService;
import io.shulie.takin.web.biz.utils.CategoryUtils;
import io.shulie.takin.web.biz.utils.PageUtils;
import io.shulie.takin.web.common.common.Response;
import io.shulie.takin.web.common.constant.ScriptManageConstant;
import io.shulie.takin.web.common.context.OperationLogContextHolder;
import io.shulie.takin.web.common.enums.activity.BusinessTypeEnum;
import io.shulie.takin.web.common.enums.scene.SceneTypeEnum;
import io.shulie.takin.web.common.enums.script.FileTypeEnum;
import io.shulie.takin.web.common.enums.script.ScriptMVersionEnum;
import io.shulie.takin.web.common.enums.script.ScriptTypeEnum;
import io.shulie.takin.web.common.exception.TakinWebException;
import io.shulie.takin.web.common.exception.TakinWebExceptionEnum;
import io.shulie.takin.web.common.util.ActivityUtil;
import io.shulie.takin.web.data.dao.scriptmanage.ScriptManageDAO;
import io.shulie.takin.web.data.param.linkmanage.*;
import io.shulie.takin.web.data.result.scriptmanage.ScriptManageResult;
import io.shulie.takin.web.diff.api.DiffFileApi;
import io.shulie.takin.web.diff.api.scenemanage.SceneManageApi;
import io.shulie.takin.web.ext.util.WebPluginUtils;
import io.shulie.takin.web.data.dao.activity.ActivityDAO;
import io.shulie.takin.web.data.dao.application.ApplicationDAO;
import io.shulie.takin.web.data.dao.linkmanage.BusinessLinkManageDAO;
import io.shulie.takin.web.data.dao.linkmanage.LinkManageDAO;
import io.shulie.takin.web.data.dao.linkmanage.SceneDAO;
import io.shulie.takin.web.data.dao.scene.SceneLinkRelateDAO;
import io.shulie.takin.web.data.param.activity.ActivityQueryParam;
import io.shulie.takin.web.data.param.scene.SceneLinkRelateParam;
import io.shulie.takin.web.data.result.activity.ActivityListResult;
import io.shulie.takin.web.data.result.application.ApplicationResult;
import io.shulie.takin.web.data.result.application.LibraryResult;
import io.shulie.takin.web.data.result.linkmange.BusinessLinkResult;
import io.shulie.takin.web.data.result.linkmange.LinkManageResult;
import io.shulie.takin.web.data.result.linkmange.SceneResult;
import io.shulie.takin.web.data.result.linkmange.TechLinkResult;
import io.shulie.takin.web.data.result.scene.SceneLinkRelateResult;
import io.shulie.takin.web.ext.entity.UserExt;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * @author vernon
 * @date 2019/11/29 14:43
 */
@Slf4j
@Component
public class LinkManageServiceImpl implements LinkManageService {
    //事物
    @Resource(name = "transactionManager")
    DataSourceTransactionManager transactionManager;
    //技术链路管理表
    @Resource
    private TLinkManageTableMapper tLinkManageTableMapper;
    //业务链路管理表
    @Resource
    private TBusinessLinkManageTableMapper tBusinessLinkManageTableMapper;
    //场景链路关联表
    @Resource
    private TSceneLinkRelateMapper tSceneLinkRelateMapper;
    //场景表
    @Resource
    private TSceneMapper tSceneMapper;
    @Resource
    private TLinkGuardMapper tLinkGuardMapper;
    //中间件信息
    @Resource
    private TMiddlewareInfoMapper tMiddlewareInfoMapper;
    //中间件链路关联
    @Resource
    private TMiddlewareLinkRelateMapper tMiddlewareLinkRelateMapper;
    @Resource
    private AgentPluginSupportService agentPluginSupportService;
    @Autowired
    private BusinessLinkManageDAO businessLinkManageDAO;
    @Autowired
    private ApplicationDAO applicationDAO;
    @Autowired
    private LinkManageDAO linkManageDAO;
    @Autowired
    private SceneDAO sceneDAO;
    @Autowired
    private ActivityDAO activityDAO;
    @Autowired
    private SceneLinkRelateDAO sceneLinkRelateDAO;
    @Autowired
    private ScriptManageService scriptManageService;
    @Autowired
    private ScriptManageDAO scriptManageDAO;
    @Autowired
    private SceneManageApi sceneManageApi;
    @Value("${file.upload.tmp.path:/tmp/takin/}")
    private String tmpFilePath;
    @Autowired
    private SceneService sceneService;
    @Autowired
    private DiffFileApi fileApi;

    private static void iteratorChildNodes(TopologicalGraphNode parentNode,
                                           List<Category> childList,
                                           List<TopologicalGraphNode> nodes,
                                           List<TopologicalGraphRelation> relations) {
        if (CollectionUtils.isEmpty(childList)) {
            return;
        }
        List<Category> filterChildList = childList.stream().filter(distinctByName(c -> c.getApplicationName())).collect(
                Collectors.toList());
        int index = 0;
        for (int i = 0; i < filterChildList.size(); i++) {
            TopologicalGraphNode childNode = new TopologicalGraphNode();
            childNode.setKey(parentNode.getKey() + "." + index);
            NodeClassEnum nodeClassEnum = getNodeClassEnumByApplicationName(
                    filterChildList.get(i).getApplicationName());
            MiddlewareTypeEnum middlewareTypeEnum = getMiddlewareTypeEnumByApplicationName(
                    filterChildList.get(i).getApplicationName());
            childNode.setNodeType(nodeClassEnum.getCode());
            childNode.setNodeClass(nodeClassEnum.getDesc());
            if (middlewareTypeEnum != null) {
                childNode.setMiddlewareType(middlewareTypeEnum.getCode());
                childNode.setMiddlewareName(middlewareTypeEnum.getDesc());
            }
            childNode.setNodeName(filterChildList.get(i).getApplicationName());
            childNode.setNodeList(filterChildList.get(i).getNodeList());
            childNode.setUnKnowNodeList(filterChildList.get(i).getUnKnowNodeList());
            nodes.add(childNode);
            TopologicalGraphRelation relation = new TopologicalGraphRelation();
            relation.setFrom(parentNode.getKey());
            relation.setTo(childNode.getKey());
            relations.add(relation);
            if (CollectionUtils.isNotEmpty(filterChildList.get(i).getChildren())) {
                iteratorChildNodes(childNode, filterChildList.get(i).getChildren(), nodes, relations);
            }
            index++;
        }
    }

    public static NodeClassEnum getNodeClassEnumByApplicationName(String applicationName) {
        switch (applicationName) {
            case "DB中间件":
            case "MYSQL":
            case "MYSQL中间件":
            case "ORACLE":
            case "ORACLE中间件":
            case "SQLSERVER":
            case "SQLSERVER中间件":
            case "CASSANDRA":
            case "CASSANDRA中间件":
            case "HBASE":
            case "HBASE中间件":
            case "MONGODB":
            case "MONGODB中间件":
                return NodeClassEnum.DB;
            case "ELASTICSEARCH":
            case "ELASTICSEARCH中间件":
                return NodeClassEnum.ES;
            case "REDIS":
            case "REDIS中间件":
            case "MEMCACHE":
            case "MEMCACHE中间件":
                return NodeClassEnum.CACHE;
            case "ROCKETMQ":
            case "ROCKETMQ中间件":
            case "KAFKA":
            case "KAFKA中间件":
            case "ACTIVEMQ":
            case "ACTIVEMQ中间件":
            case "RABBITMQ":
            case "RABBITMQ中间件":
                return NodeClassEnum.MQ;
            case "DUBBO":
            case "DUBBO中间件":
                return NodeClassEnum.APP;
            default:
                if (applicationName.contains("未知")) {
                    return NodeClassEnum.UNKNOWN;
                }
                return NodeClassEnum.APP;
        }
    }

    public static MiddlewareTypeEnum getMiddlewareTypeEnumByApplicationName(String applicationName) {
        switch (applicationName) {
            case "MYSQL":
            case "MYSQL中间件":
                return MiddlewareTypeEnum.MySQL;
            case "ORACLE":
            case "ORACLE中间件":
                return MiddlewareTypeEnum.Oracle;
            case "SQLSERVER":
            case "SQLSERVER中间件":
                return MiddlewareTypeEnum.SQLServer;
            case "CASSANDRA":
            case "CASSANDRA中间件":
                return MiddlewareTypeEnum.Cassandra;
            case "HBASE":
            case "HBASE中间件":
                return MiddlewareTypeEnum.HBase;
            case "MONGODB":
            case "MONGODB中间件":
                return MiddlewareTypeEnum.MongoDB;
            case "ELASTICSEARCH":
            case "ELASTICSEARCH中间件":
                return MiddlewareTypeEnum.Elasticsearch;
            case "REDIS":
            case "REDIS中间件":
                return MiddlewareTypeEnum.Redis;
            case "MEMCACHE":
            case "MEMCACHE中间件":
                return MiddlewareTypeEnum.Memcache;
            case "ROCKETMQ":
            case "ROCKETMQ中间件":
                return MiddlewareTypeEnum.RocketMQ;
            case "KAFKA":
            case "KAFKA中间件":
                return MiddlewareTypeEnum.Kafka;
            case "ACTIVEMQ":
            case "ACTIVEMQ中间件":
                return MiddlewareTypeEnum.ActiveMQ;
            case "RABBITMQ":
            case "RABBITMQ中间件":
                return MiddlewareTypeEnum.RabbitMQ;
            case "DUBBO":
            case "DUBBO中间件":
                return MiddlewareTypeEnum.Dubbo;
            default:
                return null;
        }
    }

    static <T> Predicate<T> distinctByName(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    @Override
    public TechLinkResponse fetchLink(String applicationName, String entrance) {
        //TechLinkResponse techLinkResponse = linkManage.getApplicationLink(entrance, applicationName);
        //获取中间件
        //return techLinkResponse;
        return null;
    }

    @Override
    public TopologicalGraphVo fetchGraph(String body) {
        TopologicalGraphVo topologicalGraphVo = new TopologicalGraphVo();
        if (StringUtils.isNotBlank(body)) {
            Category category = JSON.parseObject(body, Category.class);
            List<TopologicalGraphNode> nodes = new ArrayList<>();
            List<TopologicalGraphRelation> relations = new ArrayList<>();

            TopologicalGraphNode userNode = new TopologicalGraphNode();
            userNode.setKey("0");
            userNode.setNodeType(NodeClassEnum.OTHER.getCode());
            userNode.setNodeClass(NodeClassEnum.OTHER.getDesc());
            userNode.setNodeName("user");
            nodes.add(userNode);

            TopologicalGraphNode rootNode = new TopologicalGraphNode();
            rootNode.setKey("1");
            rootNode.setNodeType(NodeClassEnum.APP.getCode());
            rootNode.setNodeClass(NodeClassEnum.APP.getDesc());
            rootNode.setNodeName(category.getApplicationName());
            nodes.add(rootNode);

            TopologicalGraphRelation rootRelation = new TopologicalGraphRelation();
            rootRelation.setFrom(userNode.getKey());
            rootRelation.setTo(rootNode.getKey());
            relations.add(rootRelation);

            List<Category> childList = category.getChildren();
            iteratorChildNodes(rootNode, childList, nodes, relations);
            topologicalGraphVo.setGraphNodes(nodes);
            topologicalGraphVo.setGraphRelations(relations);
        }
        return topologicalGraphVo;
    }

    @Override
    public Response getBussisnessLinks(BusinessQueryVo vo) {
        List<BusinessActiveViewListDto> result = Lists.newArrayList();
        LinkQueryVo queryVo = new LinkQueryVo();
        queryVo.setMiddleWareVersion(vo.getVersion());
        queryVo.setMiddleWareName(vo.getMiddleWareName());
        queryVo.setMiddleWareType(vo.getMiddleWareType());
        queryVo.setEntrance(vo.getEntrance());
        queryVo.setName(vo.getBusinessLinkName());
        queryVo.setIsChange(vo.getIschange());
        queryVo.setSystemProcessName(vo.getTechLinkName());
        queryVo.setDomain(vo.getDomain());
        List<BusinessLinkDto> queryResult = tBusinessLinkManageTableMapper.selectBussinessLinkListBySelective2(queryVo);
        //用户ids
        List<Long> userIds = queryResult.stream().filter(data -> null != data.getUserId()).map(
                BusinessLinkDto::getUserId).collect(Collectors.toList());
        //用户信息Map key:userId  value:user对象
        Map<Long, UserExt> userMap = WebPluginUtils.getUserMapByIds(userIds);

        List<BusinessLinkDto> pageData = PageUtils.getPage(true, vo.getCurrentPage(), vo.getPageSize(), queryResult);
        if (CollectionUtils.isNotEmpty(pageData) && pageData.size() > 0) {
            pageData.forEach(
                    single -> {
                        BusinessActiveViewListDto dto = new BusinessActiveViewListDto();
                        dto.setBusinessActiceId(single.getId());
                        dto.setBusinessActiveName(single.getLinkName());
                        dto.setCandelete(single.getCandelete());
                        dto.setCreateTime(single.getCreateTime());
                        dto.setIschange(single.getIschange());
                        //负责人id
                        dto.setUserId(single.getUserId());
                        //负责人name
                        String userName = Optional.ofNullable(userMap.get(single.getUserId()))
                                .map(UserExt::getName)
                                .orElse("");
                        dto.setUserName(userName);
                        WebPluginUtils.fillQueryResponse(dto);
                        //新版本设置业务域
                        if (StringUtils.isNotBlank(single.getBusinessDomain())) {
                            String desc = DictionaryCache.getObjectByParam("domain",
                                    Integer.parseInt(single.getBusinessDomain())).getLabel();
                            if (StringUtils.isNotBlank(desc)) {
                                dto.setBusinessDomain(desc);
                            } else {
                                //兼容历史版本
                                LinkDomainEnum domainEnum = LinkDomainEnumMapping.getByCode(single.getBusinessDomain());
                                dto.setBusinessDomain(domainEnum == null ? null : domainEnum.getDesc());
                            }
                        }
                        TechLinkDto techLinkDto = single.getTechLinkDto();

                        if (techLinkDto != null) {
                            List<TMiddlewareInfo> middlewareInfos =
                                    tMiddlewareInfoMapper.selectBySystemProcessId(techLinkDto.getLinkId());
                            List<String> middleWareStrings = middlewareInfos
                                    .stream()
                                    .map(entity ->
                                            entity.getMiddlewareName() + " " + entity.getMiddlewareVersion()
                                    ).collect(Collectors.toList());
                            dto.setMiddleWareList(middleWareStrings);
                            dto.setSystemProcessName(single.getTechLinkDto().getTechLinkName());
                        }
                        result.add(dto);
                    }
            );
        }
        return Response.success(result, CollectionUtils.isEmpty(queryResult) ? 0 : queryResult.size());
    }

    private void convertBusinessLinkResponse(BusinessLinkResult businessLinkResult,
                                             BusinessLinkResponse businessLinkResponse) {
        businessLinkResponse.setId(businessLinkResult.getId());
        businessLinkResponse.setLinkName(businessLinkResult.getLinkName());
        businessLinkResponse.setEntrance(businessLinkResult.getEntrace());
        businessLinkResponse.setIschange(businessLinkResult.getIschange());
        businessLinkResponse.setCreateTime(businessLinkResult.getCreateTime());
        businessLinkResponse.setUpdateTime(businessLinkResult.getUpdateTime());
        businessLinkResponse.setCandelete(businessLinkResult.getCandelete());
        businessLinkResponse.setIsCore(businessLinkResult.getIsCore());
        businessLinkResponse.setLinkLevel(businessLinkResult.getLinkLevel());
        businessLinkResponse.setBusinessDomain(businessLinkResult.getBusinessDomain());

        TechLinkResponse techLinkResponse = new TechLinkResponse();
        businessLinkResponse.setTechLinkResponse(techLinkResponse);
        TechLinkResult techLinkResult = businessLinkResult.getTechLinkResult();
        techLinkResponse.setLinkId(techLinkResult.getLinkId());
        techLinkResponse.setTechLinkName(techLinkResult.getTechLinkName());
        techLinkResponse.setIsChange(techLinkResult.getIsChange());
        techLinkResponse.setChange_remark(techLinkResult.getChangeRemark());
        techLinkResponse.setBody_before(techLinkResult.getBodyBefore());
        techLinkResponse.setBody_after(techLinkResult.getBodyAfter());
        techLinkResponse.setChangeType(techLinkResult.getChangeType());
    }

    @Override
    public BusinessLinkResponse getBussisnessLinkDetail(String id) {
        if (null == id) {
            throw new RuntimeException("主键不能为空");
        }
        LinkQueryVo queryVo = new LinkQueryVo();
        queryVo.setId(Long.parseLong(id));
        BusinessLinkResult businessLinkResult = businessLinkManageDAO.selectBussinessLinkById(Long.parseLong(id));

        if (Objects.nonNull(businessLinkResult)) {
            BusinessLinkResponse businessLinkResponse = new BusinessLinkResponse();
            convertBusinessLinkResponse(businessLinkResult, businessLinkResponse);
            if (businessLinkResponse.getTechLinkResponse() != null) {
                Long systemProcessId = businessLinkResponse.getTechLinkResponse().getLinkId();
                if (systemProcessId != null) {
                    LinkManageResult linkManageResult = linkManageDAO.selectLinkManageById(
                            businessLinkResult.getTechLinkResult().getLinkId());
                    businessLinkResponse.getTechLinkResponse().setMiddleWareResponses(
                            getMiddleWareResponses(linkManageResult.getApplicationName()));
                    //处理系统流程前端展示数据
                    TechLinkResponse techLinkResponse = businessLinkResponse.getTechLinkResponse();
                    String linkBody = null;
                    if (StringUtils.isNotBlank(techLinkResponse.getBody_after())) {
                        linkBody = techLinkResponse.getBody_after();
                    } else {
                        linkBody = techLinkResponse.getBody_before();
                    }
                    if (linkBody != null) {
                        Category category = JSON.parseObject(linkBody, Category.class);
                        CategoryUtils.assembleVo(category);
                        List<Category> list = new ArrayList<>();
                        list.add(category);
                        businessLinkResponse.getTechLinkResponse().setLinkNode(JSON.toJSONString(list));
                    }
                    TopologicalGraphEntity topologicalGraphEntity = new TopologicalGraphEntity();
                    if (StringUtils.isNotBlank(techLinkResponse.getBody_before())) {
                        TopologicalGraphVo topologicalGraphBeforeVo = fetchGraph(techLinkResponse.getBody_before());
                        topologicalGraphEntity.setTopologicalGraphBeforeVo(topologicalGraphBeforeVo);
                    }
                    if (StringUtils.isNotBlank(techLinkResponse.getBody_after())) {
                        TopologicalGraphVo topologicalGraphAfterVo = fetchGraph(techLinkResponse.getBody_after());
                        topologicalGraphEntity.setTopologicalGraphAfterVo(topologicalGraphAfterVo);
                    }
                    businessLinkResponse.getTechLinkResponse().setTopologicalGraphEntity(topologicalGraphEntity);
                }
            }
            return businessLinkResponse;
        }
        return new BusinessLinkResponse();
    }

    @Override
    public Response deleteScene(String sceneId) {
        //手动控制事物,减小事物的范围
        if (null == sceneId) {
            throw new TakinWebException(TakinWebExceptionEnum.SCENE_VALIDATE_ERROR, "primary key cannot be null.");
        }
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        TransactionStatus status = transactionManager.getTransaction(def);

        try {
            tSceneMapper.deleteByPrimaryKey(Long.parseLong(sceneId));

            //取出关联的业务活动id是否可以被删除
            List<SceneLinkRelate> relates = tSceneLinkRelateMapper.selectBySceneId(Long.parseLong(sceneId));
            List<Long> businessLinkIds = relates.stream().map(relate -> {
                if (relate.getBusinessLinkId() != null) {
                    return Long.parseLong(relate.getBusinessLinkId());
                }
                return 0L;
            }).collect(Collectors.toList());

            //删除关联表
            tSceneLinkRelateMapper.deleteBySceneId(sceneId);
            //过滤出可以设置为删除状态的业务活动id并设置为可以删除
            enableBusinessActiveCanDelte(businessLinkIds);

            transactionManager.commit(status);
            return Response.success();
        } catch (Exception e) {
            transactionManager.rollback(status);
            log.error(e.getMessage(), e);
            throw new TakinWebException(TakinWebExceptionEnum.SCENE_DELETE_ERROR, "删除场景失败");
        } finally {
        }
    }

    private void enableBusinessActiveCanDelte(List<Long> businessLinkIds) {
        if (CollectionUtils.isEmpty(businessLinkIds)) {
            return;
        }
        List<Long> candeletedList = businessLinkIds.stream()
                .map(single -> {
                    long count = tSceneLinkRelateMapper.countByBusinessLinkId(single);
                    if (!(count > 0)) {
                        return single;
                    }
                    return 0L;
                }).collect(Collectors.toList());

        tBusinessLinkManageTableMapper.cannotdelete(candeletedList, 0L);
    }

    @Override
    public Response<List<SceneDto>> getScenes(SceneQueryVo vo) {
        List<SceneDto> sceneDtos = tSceneMapper.selectByRelatedQuery(vo);
        List<SceneDto> pageData = PageUtils.getPage(true, vo.getCurrentPage(), vo.getPageSize(), sceneDtos);

        //查询业务活动是否存在虚拟业务活动
        List<String> sceneIds = pageData.stream().map(SceneDto::getId).map(String::valueOf).collect(
                Collectors.toList());
        SceneLinkRelateParam relateParam = new SceneLinkRelateParam();
        relateParam.setSceneIds(sceneIds);
        List<SceneLinkRelateResult> relateResults = sceneLinkRelateDAO.getList(relateParam);
        // 流程 -> 业务活动
        Map<String, List<ActivityListResult>> map = Maps.newHashMap();
        if (relateResults.size() > 0) {
            ActivityQueryParam param = new ActivityQueryParam();
            param.setBusinessType(BusinessTypeEnum.VIRTUAL_BUSINESS.getType());
            param.setActivityIds(relateResults.stream().map(SceneLinkRelateResult::getBusinessLinkId)
                    .map(Long::parseLong).collect(Collectors.toList()));
            List<ActivityListResult> results = activityDAO.getActivityList(param);
            map = relateResults.stream().collect(
                    Collectors.toMap(
                            SceneLinkRelateResult::getSceneId,
                            data -> results.stream()
                                    .filter(activity -> data.getBusinessLinkId().equals(String.valueOf(activity.getActivityId())))
                                    .collect(Collectors.toList()),
                            (List<ActivityListResult> newValueList, List<ActivityListResult> oldValueList) -> {
                                oldValueList.addAll(newValueList);
                                return oldValueList;
                            }));
        }

        Map<String, List<ActivityListResult>> finalMap = map;

        //用户ids
        List<Long> userIds = sceneDtos.stream().filter(data -> null != data.getUserId()).map(SceneDto::getUserId)
                .collect(Collectors.toList());
        Map<Long, UserExt> userExtMap = WebPluginUtils.getUserMapByIds(userIds);
        pageData = pageData.stream().map(single -> {
            int count = tSceneLinkRelateMapper.countBySceneId(single.getId());
            // 填充虚拟字段
            List<ActivityListResult> activityListResults = finalMap.get(String.valueOf(single.getId()));
            if (activityListResults != null && activityListResults.size() > 0) {
                single.setBusinessType(BusinessTypeEnum.VIRTUAL_BUSINESS.getType());
            } else {
                single.setBusinessType(BusinessTypeEnum.NORMAL_BUSINESS.getType());
            }
            single.setTechLinkCount(count);
            single.setBusinessLinkCount(count);
            String userName = WebPluginUtils.getUserName(single.getUserId(), userExtMap);
            single.setUserName(userName);
            WebPluginUtils.fillQueryResponse(single);
            return single;
        }).collect(Collectors.toList());
        return Response.success(pageData, CollectionUtils.isEmpty(sceneDtos) ? 0 : sceneDtos.size());

    }

    @Override
    public Response getMiddleWareInfo(StatisticsQueryVo vo) {
        try {
            List<LinkRemarkmiddleWareDto> list = tMiddlewareInfoMapper.selectforstatistics(vo);
            List<LinkRemarkmiddleWareDto> pageData = PageUtils.getPage(true, vo.getCurrentPage(), vo.getPageSize(),
                    list);

            pageData = pageData.stream().map(
                    single -> {
                        long id = single.getMiddleWareId();
                        List<String> techLinkIds = tMiddlewareLinkRelateMapper.selectTechIdsByMiddleWareIds(id);
                        single.setSystemProcessCount(String.valueOf(techLinkIds.size()));
                        //统计业务流程条数
                        if (CollectionUtils.isNotEmpty(techLinkIds)) {
                            int countBusinessProcess = tSceneLinkRelateMapper.countByTechLinkIds(techLinkIds);
                            single.setBussinessProcessCount(String.valueOf(countBusinessProcess));
                        }
                        return single;
                    }
            ).collect(Collectors.toList());

            return Response.success(pageData, CollectionUtils.isEmpty(list) ? 0 : list.size());
        } catch (Exception e) {
            return Response.fail("0", e.getMessage(), null);
        }

    }

    @Override
    public LinkRemarkDto getstatisticsInfo() {
        //  List<LinkRemarkmiddleWareDto> middlewareInfo = middlewareInfoMapper.selectforstatistics(null);
        LinkRemarkDto dto = new LinkRemarkDto();
        /*  dto.setLinkRemarkmiddleWareDtos(middlewareInfo);*/

        long businessProcessCount = tSceneMapper.count();
        long businessActiveCount = tBusinessLinkManageTableMapper.count();
        long systemProcessCount = tLinkManageTableMapper.countTotal();
        long systemChangeCount = tLinkManageTableMapper.countChangeNum();
        long onLineApplicationCount = tLinkManageTableMapper.countApplication();
        long linkGuardCount = tLinkGuardMapper.countGuardNum();
        dto.setBusinessProcessCount(String.valueOf(businessProcessCount));
        dto.setBusinessActiveCount(String.valueOf(businessActiveCount));
        dto.setSystemProcessCount(String.valueOf(systemProcessCount));
        dto.setSystemChangeCount(String.valueOf(systemChangeCount));
        dto.setOnLineApplicationCount(String.valueOf(onLineApplicationCount));
        dto.setLinkGuardCount(String.valueOf(linkGuardCount));
        return dto;
    }

    @Override
    public LinkHistoryInfoDto getChart() {

        LinkHistoryInfoDto dto = new LinkHistoryInfoDto();

        String begin = DateUtils.preYear(new java.util.Date());
        String end = new SimpleDateFormat("yyyy-MM").format(new java.util.Date());
        //获取过去一年到现在的日期集合
        List<Date> dateRange = DateUtils.getRangeSet2(begin, end);

        List<BusinessCoverDto> businessCoverList = new ArrayList<>();
        dateRange.stream().forEach(date -> {
            BusinessCoverDto businessCoverDto = new BusinessCoverDto();
            businessCoverDto.setMonth(DateUtils.dateToString(date));
            long count = tSceneMapper.countByTime(date);
            businessCoverDto.setCover(String.valueOf(count));
            businessCoverList.add(businessCoverDto);
        });
        dto.setBusinessCover(businessCoverList);

        List<SystemProcessDto> systemProcessList = Lists.newArrayList();
        dateRange.stream().forEach(date -> {
            SystemProcessDto systemProcessDto = new SystemProcessDto();
            systemProcessDto.setMonth(DateUtils.dateToString(date));
            long count = tLinkManageTableMapper.countSystemProcessByTime(date);
            systemProcessDto.setCover(String.valueOf(count));
            systemProcessList.add(systemProcessDto);
        });
        dto.setSystemProcess(systemProcessList);

        List<ApplicationRemoteDto> applicationRemoteList = Lists.newArrayList();
        dateRange.stream().forEach(date -> {
            ApplicationRemoteDto applicationRemoteDto = new ApplicationRemoteDto();
            applicationRemoteDto.setMonth(DateUtils.dateToString(date));
            long count = tLinkManageTableMapper.countApplicationByTime(date);
            applicationRemoteDto.setCover(String.valueOf(count));
            applicationRemoteList.add(applicationRemoteDto);
        });
        dto.setApplicationRemote(applicationRemoteList);

        Long businessFlowTotalCountNum = tSceneMapper.count();
        String businessFlowTotalCount = String.valueOf(businessFlowTotalCountNum);
        String businessFlowPressureCount = "0";
        String businessFlowPressureRate =
                (businessFlowTotalCountNum == 0L || "0".equals(businessFlowPressureCount)) ?
                        "0" : String.valueOf(businessFlowTotalCountNum / Long.parseLong(businessFlowPressureCount));
        dto.setBusinessFlowTotalCount(businessFlowTotalCount);
        dto.setBusinessFlowPressureCount(businessFlowPressureCount);
        dto.setBusinessFlowPressureRate(businessFlowPressureRate);

        // TODO: 2020/1/7 暂时统计系统流程总数
        long applicationTotalCountNum = tLinkManageTableMapper.countTotal();
        String applicationTotalCount = String.valueOf(applicationTotalCountNum);
        String applicationPressureCount = "0";
        String applicationPressureRate = (applicationTotalCountNum == 0L || applicationPressureCount.equals("0")) ?
                "0" : String.valueOf(applicationTotalCountNum / Long.parseLong(applicationPressureCount));
        dto.setApplicationTotalCount(applicationTotalCount);
        dto.setApplicationPressureCount(applicationPressureCount);
        dto.setApplicationPressureRate(applicationPressureRate);

        return dto;
    }

    @Override
    public List<MiddleWareEntity> businessProcessMiddleWares(List<String> ids) {
        List<MiddleWareEntity> result = Lists.newArrayList();

        List<Long> businessIds =
                ids.stream().map(id -> Long.parseLong(String.valueOf(id))).collect(Collectors.toList());
        //查系统流程id集合
        List<String> techIds = tBusinessLinkManageTableMapper.selectTechIdsByBusinessIds(businessIds);
        if (CollectionUtils.isEmpty(techIds)) {
            return result;
        }
        //查中间件id集合
        List<String> middleWareIds = tMiddlewareLinkRelateMapper.selectMiddleWareIdsByTechIds(techIds);
        if (CollectionUtils.isEmpty(middleWareIds)) {
            return result;
        }
        //查中间件信息
        List<Long> midllewareIdslong = middleWareIds.stream()
                .map(id -> Long.parseLong(id)).collect(Collectors.toList());
        List<MiddleWareEntity> middleWareEntities = tMiddlewareInfoMapper.selectByIds(midllewareIdslong);

        result = middleWareEntities;

        return result;
    }

    @Override
    public List<MiddleWareEntity> getAllMiddleWareTypeList() {
        List<MiddleWareEntity> result = Lists.newArrayList();
        List<TMiddlewareInfo> infos = tMiddlewareInfoMapper
                .selectBySelective(new TMiddlewareInfo());

        //按照中间件类型去重
        infos.stream().forEach(info -> {
            MiddleWareEntity entity = new MiddleWareEntity();
            entity.setId(info.getId());
            entity.setMiddleWareType(info.getMiddlewareType());
            entity.setVersion(info.getMiddlewareVersion());
            entity.setMiddleWareName(info.getMiddlewareName());
            result.add(entity);
        });
        List<MiddleWareEntity> distinct = result.stream()
                .collect(Collectors.collectingAndThen(Collectors.toCollection(
                                () -> new TreeSet<>(
                                        Comparator.comparing(MiddleWareEntity::getMiddleWareType))),
                        ArrayList::new));

        return distinct;
    }

    @Override
    public List<SystemProcessIdAndNameDto> ggetAllSystemProcess(String systemProcessName) {
        List<SystemProcessIdAndNameDto> result = Lists.newArrayList();
        LinkManageQueryParam queryParam = new LinkManageQueryParam();
        WebPluginUtils.fillQueryParam(queryParam);
        queryParam.setSystemProcessName(systemProcessName);
        List<LinkManageResult> linkManageResultList = linkManageDAO.selectList(queryParam);
        if (CollectionUtils.isNotEmpty(linkManageResultList)) {
            linkManageResultList.stream().forEach(table -> {
                SystemProcessIdAndNameDto dto = new SystemProcessIdAndNameDto();
                dto.setId(String.valueOf(table.getLinkId()));
                dto.setSystemProcessName(table.getLinkName());
                result.add(dto);
            });
        }
        return result;
    }

    @Override
    public List<SystemProcessIdAndNameDto> getAllSystemProcessCanrelateBusiness(String systemProcessName) {
        List<SystemProcessIdAndNameDto> result = Lists.newArrayList();
        LinkManageTable serachTable = new LinkManageTable();
        serachTable.setLinkName(systemProcessName);
        serachTable.setCanDelete(0);

        List<LinkManageTable> tables =
                tLinkManageTableMapper.selectBySelective(serachTable);
        if (CollectionUtils.isNotEmpty(tables)) {
            tables.stream().forEach(table -> {
                SystemProcessIdAndNameDto dto = new SystemProcessIdAndNameDto();
                dto.setId(String.valueOf(table.getLinkId()));
                dto.setSystemProcessName(table.getLinkName());
                result.add(dto);

            });
        }
        return result;
    }

    @Override
    public List<String> entranceFuzzSerach(String entrance) {
        List<String> entrances = tLinkManageTableMapper.entranceFuzzSerach(entrance);
        return entrances;
    }

    @Override
    public List<BusinessActiveIdAndNameDto> businessActiveNameFuzzSearch(String businessActiveName) {
        List<BusinessActiveIdAndNameDto> businessActiveIdAndNameDtoList = Lists.newArrayList();
        BusinessLinkManageQueryParam queryParam = new BusinessLinkManageQueryParam();
        WebPluginUtils.fillQueryParam(queryParam);
        queryParam.setBussinessActiveName(businessActiveName);
        List<BusinessLinkResult> businessLinkResultList = businessLinkManageDAO.selectList(queryParam);
        if (CollectionUtils.isNotEmpty(businessLinkResultList)) {
            businessActiveIdAndNameDtoList = businessLinkResultList.stream().map(businessLinkResult -> {
                BusinessActiveIdAndNameDto bActive = new BusinessActiveIdAndNameDto();
                bActive.setId(businessLinkResult.getId());
                bActive.setBusinessActiveName(businessLinkResult.getLinkName());
                return bActive;
            }).collect(Collectors.toList());
        }
        return businessActiveIdAndNameDtoList;
    }

    @Transactional
    @Override
    public void addBusinessFlow(BusinessFlowVo vo) throws Exception {
        //添加业务活动主表并返回业务活动的id
        if (CollectionUtils.isEmpty(vo.getRoot())) {
            throw new TakinWebException(TakinWebExceptionEnum.LINK_VALIDATE_ERROR, "关联业务活动不能为空");
        }
        Long sceneId = addScene(vo);
        List<SceneLinkRelate> relates = parsingTree(vo, sceneId);
        if (CollectionUtils.isNotEmpty(relates)) {
            //补全信息
            infoCompletion(relates);
            tSceneLinkRelateMapper.batchInsert(relates);
            //设置业务活动不能被删除
            diableDeleteBusinessActives(relates);
        }

    }

    /**
     * 设置业务活动不能被删除
     */
    private void diableDeleteBusinessActives(List<SceneLinkRelate> relates) {

        List<Long> relateBusinessLinkIds =
                relates.stream().map(
                        single -> {
                            if (single.getBusinessLinkId() != null) {
                                return Long.parseLong(single.getBusinessLinkId());
                            }
                            return 0L;
                        }
                ).collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(relateBusinessLinkIds)) {
            tBusinessLinkManageTableMapper.cannotdelete(relateBusinessLinkIds, 1L);
        }
    }

    @Override
    public List<BusinessFlowIdAndNameDto> businessFlowIdFuzzSearch(String businessFlowName) {
        List<BusinessFlowIdAndNameDto> businessFlowIdAndNameDtoList = Lists.newArrayList();
        SceneQueryParam queryParam = new SceneQueryParam();
        WebPluginUtils.fillQueryParam(queryParam);
        queryParam.setSceneName(businessFlowName);
        List<SceneResult> sceneResultList = sceneDAO.selectList(queryParam);
        if (CollectionUtils.isNotEmpty(sceneResultList)) {
            businessFlowIdAndNameDtoList = sceneResultList.stream().map(sceneResult -> {
                BusinessFlowIdAndNameDto businessFlowIdAndNameDto = new BusinessFlowIdAndNameDto();
                businessFlowIdAndNameDto.setId(String.valueOf(sceneResult.getId()));
                businessFlowIdAndNameDto.setBusinessFlowName(sceneResult.getSceneName());
                return businessFlowIdAndNameDto;
            }).collect(Collectors.toList());
        }
        return businessFlowIdAndNameDtoList;
    }

    /**
     * 解析树并返回关联表封装集合
     *
     * @return
     * @throws Exception
     */
    private List<SceneLinkRelate> parsingTree(BusinessFlowVo vo, Long sceneId) throws Exception {

        List<SceneLinkRelate> relates = Lists.newArrayList();
        //根节点集合
        List<BusinessFlowTree> roots = vo.getRoot();
        for (int i = 0; i < roots.size(); i++) {
            String parentId = null;
            BusinessFlowTree root = roots.get(i);
            String businessId = root.getId();
            if (StringUtils.isBlank(businessId)) {
                continue;
            }
            SceneLinkRelate relate = new SceneLinkRelate();
            relate.setSceneId(String.valueOf(sceneId));
            relate.setParentBusinessLinkId(parentId);
            relate.setBusinessLinkId(businessId);
            relate.setIsDeleted(0);
            //前端产生的uuid
            relate.setFrontUUIDKey(root.getKey());
            relates.add(relate);
            List<BusinessFlowTree> children = root.getChildren();
            if (CollectionUtils.isNotEmpty(children)) {
                parsing(children, businessId, sceneId, relates);
            }
        }

        return relates;
    }

    private Long addScene(BusinessFlowVo vo) {
        SceneCreateParam param = new SceneCreateParam();
        param.setSceneName(vo.getSceneName());
        param.setSceneLevel(vo.getSceneLevel());
        param.setIsCore(Integer.parseInt(vo.getIsCore()));
        param.setIsChanged(0);
        param.setIsDeleted(0);
        sceneDAO.insert(param);
        return param.getId();
    }

    /**
     * 对业务流程链路关联表的信息补全
     *
     * @param relates 业务流程链路关联集合
     */
    private void infoCompletion(List<SceneLinkRelate> relates) {
        //获取出所有的业务活动ID
        List<Long> businessIds
                = relates
                .stream()
                .map(relate -> Long.parseLong(relate.getBusinessLinkId())).collect(Collectors.toList());

        List<BusinessLinkManageTable> tables =
                tBusinessLinkManageTableMapper.selectByPrimaryKeys(businessIds);

        Map<Long, List<BusinessLinkManageTable>> map
                = tables.stream()
                .collect(Collectors.groupingBy(
                        BusinessLinkManageTable::getLinkId));

        relates.stream().forEach(
                relate -> {
                    Long businessLinkId = Long.parseLong(relate.getBusinessLinkId());
                    List<BusinessLinkManageTable> lists = map.get(businessLinkId);
                    if (CollectionUtils.isNotEmpty(lists)) {
                        BusinessLinkManageTable table = lists.get(0);
                        relate.setEntrance(table.getEntrace());
                        relate.setTechLinkId(table.getRelatedTechLink());
                    }
                }
        );
    }

    /**
     * @param children 子节点集合
     * @param parentId 父亲节点
     * @param sceneId  业务流程id
     * @param result   返回结果的集合
     * @return
     */
    private List<SceneLinkRelate> parsing(List<BusinessFlowTree> children, String parentId, Long sceneId,
                                          List<SceneLinkRelate> result) {
        for (int i = 0; i < children.size(); i++) {
            SceneLinkRelate relate = new SceneLinkRelate();
            BusinessFlowTree child = children.get(i);
            String businessId = child.getId();
            if (StringUtils.isNotBlank(businessId)) {
                relate.setBusinessLinkId(child.getId());
                relate.setParentBusinessLinkId(parentId);
                relate.setIsDeleted(0);
                relate.setFrontUUIDKey(child.getKey());
                relate.setSceneId(String.valueOf(sceneId));
                result.add(relate);
            }

            List<BusinessFlowTree> lowerChildren = children.get(i).getChildren();
            if (CollectionUtils.isNotEmpty(lowerChildren)) {
                parsing(lowerChildren, child.getId(), sceneId, result);
            }
        }
        return result;
    }

    @Override
    public BusinessFlowDto getBusinessFlowDetail(String id) {
        BusinessFlowDto dto = new BusinessFlowDto();

        //获取业务流程基本信息
        Scene scene = tSceneMapper.selectByPrimaryKey(Long.parseLong(id));
        if (Objects.isNull(scene)) {
            throw new TakinWebException(TakinWebExceptionEnum.LINK_VALIDATE_ERROR,
                    id + "对应的业务流程不存在");
        }

        dto.setId(String.valueOf(scene.getId()));
        dto.setIsCode(String.valueOf(scene.getIsCore()));
        dto.setLevel(scene.getSceneLevel());
        dto.setBusinessProcessName(scene.getSceneName());

        List<SceneLinkRelate> relates = tSceneLinkRelateMapper.selectBySceneId(Long.parseLong(id));

        List<ExistBusinessActiveDto> existBusinessActiveIds =
                relates.stream().map(relate ->
                {
                    ExistBusinessActiveDto single = new ExistBusinessActiveDto();
                    single.setKey(relate.getFrontUUIDKey());
                    single.setId(relate.getBusinessLinkId());
                    return single;
                }).collect(Collectors.toList());

        dto.setExistBusinessActive(existBusinessActiveIds);

        List<BusinessFlowTree> roots = tSceneLinkRelateMapper.findAllRecursion(id);
        dto.setRoots(roots);

        //中间件信息
        List<String> techLinkIds = relates.stream().map(relate -> relate.getTechLinkId()).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(techLinkIds)) {
            List<String> middleWareIdStrings = tMiddlewareLinkRelateMapper.selectMiddleWareIdsByTechIds(techLinkIds);
            List<Long> middleWareIds = middleWareIdStrings.stream().map(single -> Long.parseLong(single)).collect(
                    Collectors.toList());

            List<MiddleWareEntity> middleWareEntityList = Lists.newArrayList();
            if (CollectionUtils.isNotEmpty(middleWareIds)) {
                middleWareEntityList = tMiddlewareInfoMapper.selectByIds(middleWareIds);
            }

            dto.setMiddleWareEntities(middleWareEntityList);
        }

        return dto;

    }

    @Transactional
    @Override
    public void modifyBusinessFlow(BusinessFlowVo vo) throws Exception {
        if (CollectionUtils.isEmpty(vo.getRoot())) {
            throw new TakinWebException(TakinWebExceptionEnum.LINK_VALIDATE_ERROR, "关联业务活动不能为空");
        }
        recordBusinessFlowLog(vo);
        //修改主表
        modifyScene(vo);
        //激活可以业务活动可以删除
        enableBusinessDelete(vo);
        //删除老的关联信息
        tSceneLinkRelateMapper.deleteBySceneId(vo.getId());
        //重新生成关联信息
        List<SceneLinkRelate> relates = parsingTree(vo, Long.parseLong(vo.getId()));
        if (CollectionUtils.isNotEmpty(relates)) {
            //补全信息
            infoCompletion(relates);
            tSceneLinkRelateMapper.batchInsert(relates);
        }
        //冻结业务活动可以删除
        diableDeleteBusinessActives(relates);
    }

    private void recordBusinessFlowLog(BusinessFlowVo vo) throws Exception {
        //记录变更日志
        Scene oldScene = tSceneMapper.selectByPrimaryKey(Long.parseLong(vo.getId()));
        OperationLogContextHolder.addVars(BizOpConstants.Vars.BUSINESS_PROCESS, vo.getSceneName());
        List<SceneLinkRelate> oldSceneLinkRelateList = tSceneLinkRelateMapper.selectBySceneId(
                Long.parseLong(vo.getId()));
        List<Long> oldBusinessLinkIdList = oldSceneLinkRelateList.stream().map(SceneLinkRelate::getBusinessLinkId).map(
                Long::parseLong).collect(Collectors.toList());
        List<SceneLinkRelate> currentSceneLinkRelateList = parsingTree(vo, Long.parseLong(vo.getId()));
        List<Long> currentBusinessLinkIdList = currentSceneLinkRelateList.stream().map(
                SceneLinkRelate::getBusinessLinkId).map(Long::parseLong).collect(Collectors.toList());
        List<Long> toDeleteIdList = Lists.newArrayList();
        toDeleteIdList.addAll(oldBusinessLinkIdList);
        toDeleteIdList.removeAll(currentBusinessLinkIdList);
        List<Long> toAddIdList = Lists.newArrayList();
        toAddIdList.addAll(currentBusinessLinkIdList);
        toAddIdList.removeAll(oldBusinessLinkIdList);
        String selectiveContent = "";
        if (oldScene.getSceneName().equals(vo.getSceneName())
                && CollectionUtils.isEmpty(toAddIdList)
                && CollectionUtils.isEmpty(toDeleteIdList)) {
            OperationLogContextHolder.ignoreLog();
        }
        if (CollectionUtils.isNotEmpty(toAddIdList)) {
            List<BusinessLinkManageTable> businessLinkManageTableList = tBusinessLinkManageTableMapper
                    .selectBussinessLinkByIdList(toAddIdList);
            if (CollectionUtils.isNotEmpty(businessLinkManageTableList)) {
                String addNodeNames = businessLinkManageTableList.stream().map(BusinessLinkManageTable::getLinkName)
                        .collect(Collectors.joining(","));
                selectiveContent = selectiveContent + "｜新增节点：" + addNodeNames;
            }
        }
        if (CollectionUtils.isNotEmpty(toDeleteIdList)) {
            List<BusinessLinkManageTable> businessLinkManageTableList = tBusinessLinkManageTableMapper
                    .selectBussinessLinkByIdList(toDeleteIdList);
            if (CollectionUtils.isNotEmpty(businessLinkManageTableList)) {
                String deleteNodeNames = businessLinkManageTableList.stream().map(BusinessLinkManageTable::getLinkName)
                        .collect(Collectors.joining(","));
                selectiveContent = selectiveContent + "｜删除节点：" + deleteNodeNames;
            }
        }
        OperationLogContextHolder.addVars(BizOpConstants.Vars.BUSINESS_PROCESS_SELECTIVE_CONTENT, selectiveContent);
    }

    private void enableBusinessDelete(BusinessFlowVo vo) {
        if (vo.getId() == null) {
            return;
        }
        List<SceneLinkRelate> oldRelates =
                tSceneLinkRelateMapper.selectBySceneId(Long.parseLong(vo.getId()));
        if (CollectionUtils.isEmpty(oldRelates)) {
            return;
        }

        List<Long> candeleteList = oldRelates.stream()
                .map(single ->
                {
                    if (single.getBusinessLinkId() == null) {
                        return 0L;
                    }
                    return Long.parseLong(single.getBusinessLinkId());
                }).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(candeleteList)) {
            return;
        }
        tBusinessLinkManageTableMapper.cannotdelete(candeleteList, 0L);
    }

    /**
     * 修改场景的主表
     */
    private void modifyScene(BusinessFlowVo vo) {
        String sceneId = vo.getId();
        String sceneName = vo.getSceneName();
        String isCore = vo.getIsCore();
        String sceneLevel = vo.getSceneLevel();
        Scene updateScene = new Scene();
        updateScene.setId(Long.parseLong(sceneId));
        updateScene.setSceneName(sceneName);
        updateScene.setIsCore(Integer.parseInt(isCore));
        updateScene.setSceneLevel(sceneLevel);
        tSceneMapper.updateByPrimaryKeySelective(updateScene);
    }

    @Override
    public List<MiddleWareNameDto> cascadeMiddleWareNameAndVersion(String middleWareType) throws Exception {
        List<MiddleWareNameDto> result = Lists.newArrayList();

        //拿出所有的中间件名字
        TMiddlewareInfo info = new TMiddlewareInfo();
        if (StringUtils.isNotBlank(middleWareType)) {
            info.setMiddlewareType(middleWareType);
        }
        List<TMiddlewareInfo> infos =
                tMiddlewareInfoMapper.selectBySelective(info);
        if (CollectionUtils.isNotEmpty(infos)) {
            Map<String, List<TMiddlewareInfo>> groupByMiddleWareName =
                    infos.stream().collect(Collectors.groupingBy(TMiddlewareInfo::getMiddlewareName));

            for (Map.Entry<String, List<TMiddlewareInfo>> entry : groupByMiddleWareName.entrySet()) {
                MiddleWareNameDto dto = new MiddleWareNameDto();
                String middleWareName = entry.getKey();
                dto.setLabel(middleWareName);
                dto.setValue(middleWareName);
                List<TMiddlewareInfo> values = entry.getValue();
                if (CollectionUtils.isNotEmpty(values)) {
                    List<MiddleWareVersionDto> children = values.stream().map(
                            single -> {
                                MiddleWareVersionDto versionDto = new MiddleWareVersionDto();
                                String version = single.getMiddlewareVersion();
                                versionDto.setLabel(version);
                                versionDto.setValue(version);
                                return versionDto;
                            }
                    ).collect(Collectors.toList());
                    dto.setChildren(children);
                }
                result.add(dto);
            }
        }
        return result;
    }

    @Override
    public List<MiddleWareNameDto> getDistinctMiddleWareName() {
        List<MiddleWareNameDto> result = Lists.newArrayList();

        List<TMiddlewareInfo> infos = tMiddlewareInfoMapper
                .selectBySelective(new TMiddlewareInfo());

        //按照中间件类型去重
        infos.stream().forEach(single -> {
            MiddleWareNameDto entity = new MiddleWareNameDto();
            entity.setValue(single.getMiddlewareName());
            entity.setLabel(single.getMiddlewareName());
            result.add(entity);
        });
        List<MiddleWareNameDto> distinct = result.stream()
                .collect(Collectors.collectingAndThen(Collectors.toCollection(
                                () -> new TreeSet<>(
                                        Comparator.comparing(MiddleWareNameDto::getLabel))),
                        ArrayList::new));
        return distinct;
    }

    @Override
    public List<EntranceSimpleDto> getEntranceByAppName(String applicationName) {
        return null;
    }

    @Override
    public List<MiddleWareResponse> getMiddleWareResponses(String applicationName) {
        List<MiddleWareResponse> middleWareResponses = Lists.newArrayList();
        List<AgentPluginSupportResponse> supportList = agentPluginSupportService.queryAgentPluginSupportList();
        List<ApplicationResult> applicationResultList = applicationDAO.getApplicationByName(
                Arrays.asList(applicationName));
        if (CollectionUtils.isEmpty(applicationResultList)) {
            return middleWareResponses;
        }
        LibraryResult[] libraryResults = applicationResultList.get(0).getLibrary();
        if (null == libraryResults || libraryResults.length == 0) {
            return middleWareResponses;
        }
        for (LibraryResult libraryResult : libraryResults) {
            MiddleWareResponse middleWareResponse = agentPluginSupportService.convertLibInfo(supportList,
                    libraryResult.getLibraryName());
            if (!Objects.isNull(middleWareResponse)) {
                middleWareResponses.add(middleWareResponse);
            }
        }
        middleWareResponses.sort((a, b) -> {
            if (a.getStatusResponse().getValue() > b.getStatusResponse().getValue()) {
                return 1;
            } else if (a.getStatusResponse().getValue() < b.getStatusResponse().getValue()) {
                return -1;
            } else {
                return 0;
            }
        });
        return middleWareResponses;
    }

    @Override
    public List<BusinessActivityNameResponse> getBusinessActiveByFlowId(Long businessFlowId) {
        List<BusinessActivityNameResponse> sceneBusinessActivityRefVOS = new ArrayList<>();
        List<SceneLinkRelate> sceneLinkRelates = tSceneLinkRelateMapper.selectBySceneId(businessFlowId);
        if (CollectionUtils.isNotEmpty(sceneLinkRelates)) {
            List<Long> businessActivityIds = sceneLinkRelates.stream().map(o -> Long.valueOf(o.getBusinessLinkId()))
                    .collect(Collectors.toList());
            List<BusinessLinkManageTable> businessLinkManageTables = tBusinessLinkManageTableMapper
                    .selectBussinessLinkByIdList(businessActivityIds);
            //因为businessLinkManageTables打乱了业务活动的顺序 所以使用businessActivityIds
            sceneBusinessActivityRefVOS = businessActivityIds.stream().map(activityId -> {
                BusinessActivityNameResponse businessActivityNameResponse = new BusinessActivityNameResponse();
                businessActivityNameResponse.setBusinessActivityId(activityId);
                BusinessLinkManageTable linkManageTable = businessLinkManageTables.stream().filter(
                        link -> activityId.equals(link.getLinkId())).findFirst().orElse(null);
                if (Objects.nonNull(linkManageTable)) {
                    businessActivityNameResponse.setBusinessActivityName(linkManageTable.getLinkName());
                }
                return businessActivityNameResponse;
            }).collect(Collectors.toList());
        }
        return sceneBusinessActivityRefVOS;
    }

    @Override
    public BusinessFlowDetailResponse parseScriptAndSave(BusinessFlowParseRequest businessFlowParseRequest) {
        FileManageUpdateRequest fileManageCreateRequest = businessFlowParseRequest.getScriptFile();
        //如果文件内容不为空，使用文件内容新建脚本文件
        if (StringUtils.isNotBlank(fileManageCreateRequest.getScriptContent())) {
            UUID uuid = UUID.randomUUID();
            fileManageCreateRequest.setUploadId(uuid.toString());
            String tempFile = tmpFilePath + "/" + uuid + "/" + fileManageCreateRequest.getFileName();
            FileCreateByStringParamReq fileCreateByStringParamReq = new FileCreateByStringParamReq();
            fileCreateByStringParamReq.setFileContent(fileManageCreateRequest.getScriptContent());
            fileCreateByStringParamReq.setFilePath(tempFile);
            String fileMd5 = fileApi.createFileByPathAndString(fileCreateByStringParamReq);
            fileManageCreateRequest.setMd5(fileMd5);
        }

        //解析脚本
        ScriptAnalyzeRequest analyzeRequest = new ScriptAnalyzeRequest();
        analyzeRequest.setScriptFile(tmpFilePath + "/" + fileManageCreateRequest.getUploadId() + "/" + fileManageCreateRequest.getFileName());
        ResponseResult<List<ScriptNode>> listResponseResult = sceneManageApi.scriptAnalyze(analyzeRequest);
        if (!listResponseResult.getSuccess() || CollectionUtils.isEmpty(listResponseResult.getData())) {
            throw new TakinWebException(TakinWebExceptionEnum.SCRIPT_VALIDATE_ERROR, "脚本文件解析失败！" + listResponseResult.getError().getMsg());
        }
        List<ScriptNode> data = listResponseResult.getData();
        List<ScriptNode> testPlan = JmxUtil.getScriptNodeByType(NodeTypeEnum.TEST_PLAN, data);
        if (CollectionUtils.isEmpty(testPlan)) {
            throw new TakinWebException(TakinWebExceptionEnum.SCRIPT_VALIDATE_ERROR, "脚本文件没有解析到测试计划！");
        }

        if (businessFlowParseRequest.getId() == null) {
            saveBusinessFlow(testPlan.get(0).getTestName(), data, fileManageCreateRequest);
        } else {
            updateBusinessFlow(businessFlowParseRequest.getId(), businessFlowParseRequest.getScriptFile(), null);
        }

        BusinessFlowDetailResponse result = new BusinessFlowDetailResponse();
        result.setId(businessFlowParseRequest.getId());
        return result;
    }

    @Transactional(rollbackFor = Exception.class)
    public void saveBusinessFlow(String testName, List<ScriptNode> data, FileManageUpdateRequest fileManageCreateRequest) {
        //保存业务流程
        SceneCreateParam sceneCreateParam = new SceneCreateParam();
        sceneCreateParam.setSceneName(testName);
        sceneCreateParam.setCustomerId(WebPluginUtils.getCustomerId());
        sceneCreateParam.setUserId(WebPluginUtils.getUserId());
        sceneCreateParam.setLinkRelateNum(0);
        sceneCreateParam.setScriptJmxNode(JsonHelper.bean2Json(data));
        sceneCreateParam.setTotalNodeNum(JmxUtil.getNodeNumByType(NodeTypeEnum.SAMPLER, data));
        sceneCreateParam.setType(SceneTypeEnum.JMETER_UPLOAD_SCENE.getType());
        sceneDAO.insert(sceneCreateParam);

        //新增脚本文件
        ScriptManageDeployCreateRequest createRequest = new ScriptManageDeployCreateRequest();
        //脚本文件名称去重
        String scriptName = sceneCreateParam.getSceneName();
        List<ScriptManageResult> scriptManageResults = scriptManageDAO.selectScriptManageByName(sceneCreateParam.getSceneName());
        if (CollectionUtils.isNotEmpty(scriptManageResults)) {
            scriptName = sceneCreateParam.getSceneName() + "_" + DateUtils.dateToString(new Date(), "yyyyMMddHHmmss");
        }
        createRequest.setFileManageCreateRequests(Collections.singletonList(LinkManageConvert.INSTANCE.ofFileManageCreateRequest(fileManageCreateRequest)));
        createRequest.setName(scriptName);
        createRequest.setType(ScriptTypeEnum.JMETER.getCode());
        createRequest.setMVersion(ScriptMVersionEnum.SCRIPT_M_1.getCode());
        createRequest.setRefType(ScriptManageConstant.BUSINESS_PROCESS_REF_TYPE);
        createRequest.setRefValue(sceneCreateParam.getId().toString());
        Long scriptManageId = scriptManageService.createScriptManage(createRequest);

        //更新业务流程
        SceneUpdateParam sceneUpdateParam = new SceneUpdateParam();
        sceneUpdateParam.setId(sceneCreateParam.getId());
        sceneCreateParam.setScriptDeployId(scriptManageId);
        sceneDAO.update(sceneUpdateParam);
    }


    @Override
    public BusinessFlowDetailResponse getBusinessFlowDetail(Long id) {
        BusinessFlowDetailResponse result = new BusinessFlowDetailResponse();
        SceneResult sceneResult = sceneDAO.getSceneDetail(id);
        if (sceneResult == null) {
            return result;
        }
        List<ScriptNode> scriptNodes = JsonHelper.json2List(sceneResult.getScriptJmxNode(), ScriptNode.class);
        //将节点树处理成线程组在最外层的形式
        List<ScriptNode> scriptNodeByType = JmxUtil.getScriptNodeByType(NodeTypeEnum.THREAD_GROUP, scriptNodes);
        List<ScriptJmxNode> scriptJmxNodes = LinkManageConvert.INSTANCE.ofScriptNodeList(scriptNodeByType);

        ScriptManageDeployDetailResponse scriptManageDeployDetail = scriptManageService.getScriptManageDeployDetail(sceneResult.getScriptDeployId());
        if (scriptManageDeployDetail != null) {
            //脚本文件单独存储
            if (CollectionUtils.isNotEmpty(scriptManageDeployDetail.getFileManageResponseList())) {
                List<FileManageResponse> fileManageResponses = scriptManageDeployDetail.getFileManageResponseList().stream()
                        .filter(o -> FileTypeEnum.SCRIPT.getCode().equals(o.getFileType())).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(fileManageResponses)) {
                    result.setScriptFile(LinkManageConvert.INSTANCE.ofFileManageResponse(fileManageResponses.get(0)));
                    scriptManageDeployDetail.getFileManageResponseList().remove(fileManageResponses.get(0));
                }
            }
            result = LinkManageConvert.INSTANCE.ofBusinessFlowDetailResponse(scriptManageDeployDetail);
            int fileManageNum = result.getFileManageResponseList() == null ? 0 : result.getFileManageResponseList().size();
            int attachmentManageNum = result.getAttachmentManageResponseList() == null ? 0 : result.getAttachmentManageResponseList().size();
            result.setFileNum(fileManageNum + attachmentManageNum);
        }

        result.setScriptJmxNodeList(scriptJmxNodes);
        result.setThreadGroupNum(scriptNodeByType.size());
        toBusinessFlowDetailResponse(sceneResult, result);
        return result;
    }

    @Override
    public BusinessFlowDetailResponse uploadDataFile(BusinessFlowDataFileRequest businessFlowDataFileRequest) {
        updateBusinessFlow(businessFlowDataFileRequest.getId(), null, businessFlowDataFileRequest);
        BusinessFlowDetailResponse result = new BusinessFlowDetailResponse();
        result.setId(businessFlowDataFileRequest.getId());
        return result;
    }

    @Override
    public BusinessFlowDetailResponse getThreadGroupDetail(Long id, String xpathMd5) {
        BusinessFlowDetailResponse result = new BusinessFlowDetailResponse();
        SceneResult sceneResult = sceneDAO.getSceneDetail(id);
        if (sceneResult == null) {
            return result;
        }
        List<ScriptNode> scriptNodes = JsonHelper.json2List(sceneResult.getScriptJmxNode(), ScriptNode.class);
        //将节点树处理成线程组在最外层的形式
        List<ScriptNode> scriptNodeByType = JmxUtil.getScriptNodeByType(NodeTypeEnum.THREAD_GROUP, scriptNodes);
        List<ScriptJmxNode> scriptJmxNodes = LinkManageConvert.INSTANCE.ofScriptNodeList(scriptNodeByType);
        List<ScriptJmxNode> threadJmxNode = scriptJmxNodes.stream().filter(o -> o.getXpathMd5().equals(xpathMd5)).collect(Collectors.toList());
        SceneLinkRelateParam sceneLinkRelateParam = new SceneLinkRelateParam();
        sceneLinkRelateParam.setSceneIds(Collections.singletonList(id.toString()));
        List<SceneLinkRelateResult> sceneLinkRelateList = sceneLinkRelateDAO.getList(sceneLinkRelateParam);
        dealScriptJmxNodes(sceneLinkRelateList, threadJmxNode);

        result.setScriptJmxNodeList(threadJmxNode);
        result.setThreadGroupNum(scriptNodeByType.size());
        toBusinessFlowDetailResponse(sceneResult, result);
        return result;
    }

    @Override
    public BusinessFlowMatchResponse autoMatchActivity(Long id) {
        BusinessFlowMatchResponse result = new BusinessFlowMatchResponse();
        SceneResult sceneResult = sceneDAO.getSceneDetail(id);
        if (sceneResult == null) {
            return result;
        }
        result.setId(id);
        result.setBusinessProcessName(sceneResult.getSceneName());
        List<ScriptNode> scriptNodes = JsonHelper.json2List(sceneResult.getScriptJmxNode(), ScriptNode.class);
        int nodeNumByType = JmxUtil.getNodeNumByType(NodeTypeEnum.SAMPLER, scriptNodes);
        List<SceneLinkRelateResult> sceneLinkRelateResults = sceneService.nodeLinkToBusinessActivity(scriptNodes, id);
        //查询已有的匹配关系,删除现在没有关联的节点
        SceneLinkRelateParam sceneLinkRelateParam = new SceneLinkRelateParam();
        sceneLinkRelateParam.setSceneIds(Collections.singletonList(id.toString()));
        List<SceneLinkRelateResult> sceneLinkRelateList = sceneLinkRelateDAO.getList(sceneLinkRelateParam);
        if (CollectionUtils.isNotEmpty(sceneLinkRelateList)){
            List<Long> oldIds = sceneLinkRelateList.stream().map(SceneLinkRelateResult::getId).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(sceneLinkRelateResults)){
                List<Long> longList = sceneLinkRelateResults.stream().map(SceneLinkRelateResult::getId)
                        .filter(Objects::nonNull).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(longList)){
                    oldIds = oldIds.stream().filter(o -> !longList.contains(o)).collect(Collectors.toList());
                }
            }
            sceneLinkRelateDAO.deleteByIds(oldIds);
        }

        if (CollectionUtils.isNotEmpty(sceneLinkRelateResults)){
            sceneLinkRelateDAO.batchInsertOrUpdate(LinkManageConvert.INSTANCE.ofSceneLinkRelateResults(sceneLinkRelateResults));
        }
        int matchNum = CollectionUtils.isEmpty(sceneLinkRelateResults) ? 0 : sceneLinkRelateResults.size();
        result.setMatchNum(matchNum);
        result.setUnMatchNum(nodeNumByType - matchNum);
        return result;
    }


    @Transactional(rollbackFor = Exception.class)
    public void updateBusinessFlow(Long businessFlowId, FileManageUpdateRequest scriptFile, BusinessFlowDataFileRequest businessFlowDataFileRequest) {
        SceneResult sceneResult = sceneDAO.getSceneDetail(businessFlowId);
        if (sceneResult == null) {
            throw new TakinWebException(TakinWebExceptionEnum.LINK_QUERY_ERROR, "没有找到对应的业务流程！");
        }
        //取之前脚本中关联的其他文件
        Long oldScriptDeployId = sceneResult.getScriptDeployId();
        ScriptManageDeployDetailResponse result = new ScriptManageDeployDetailResponse();
        result.setId(oldScriptDeployId);
        scriptManageService.setFileList(result);
        List<FileManageResponse> fileManageResponseList = result.getFileManageResponseList();

        ScriptManageDeployUpdateRequest updateRequest = new ScriptManageDeployUpdateRequest();
        if (scriptFile == null) {
            List<FileManageResponse> dataFileManageResponseList = fileManageResponseList.stream().filter(o ->
                    FileTypeEnum.SCRIPT.getCode().equals(o.getFileType())).collect(Collectors.toList());
            //更新脚本
            if (CollectionUtils.isEmpty(businessFlowDataFileRequest.getFileManageUpdateRequests())) {
                businessFlowDataFileRequest.setFileManageUpdateRequests(new ArrayList<>());
            }
            businessFlowDataFileRequest.getFileManageUpdateRequests().addAll(LinkManageConvert.INSTANCE
                    .ofFileManageResponseList(dataFileManageResponseList));

            updateRequest.setFileManageUpdateRequests(businessFlowDataFileRequest.getFileManageUpdateRequests());
            updateRequest.setAttachmentManageUpdateRequests(businessFlowDataFileRequest.getAttachmentManageUpdateRequests());
            updateRequest.setPluginConfigUpdateRequests(businessFlowDataFileRequest.getPluginConfigUpdateRequests());

        } else {
            List<FileManageResponse> dataFileManageResponseList = fileManageResponseList.stream().filter(o ->
                    !FileTypeEnum.SCRIPT.getCode().equals(o.getFileType())).collect(Collectors.toList());
            List<FileManageUpdateRequest> updateFileManageRequests = new ArrayList<>();
            updateFileManageRequests.add(scriptFile);
            updateFileManageRequests.addAll(LinkManageConvert.INSTANCE.ofFileManageResponseList(dataFileManageResponseList));
            updateRequest.setFileManageUpdateRequests(updateFileManageRequests);
        }

        //更新脚本
        Long scriptDeployId = scriptManageService.updateScriptManage(updateRequest);
        SceneUpdateParam sceneUpdateParam = new SceneUpdateParam();
        //更新业务流程
        sceneUpdateParam.setScriptDeployId(scriptDeployId);
        sceneDAO.update(sceneUpdateParam);
        //TODO 更新压测场景


    }

    private void toBusinessFlowDetailResponse(SceneResult sceneResult, BusinessFlowDetailResponse result) {
        result.setSceneLevel(sceneResult.getSceneLevel());
        result.setIsCode(sceneResult.getIsCore());
        result.setBusinessProcessName(sceneResult.getSceneName());
        result.setId(sceneResult.getId());
    }

    private void dealScriptJmxNodes(List<SceneLinkRelateResult> sceneLinkRelateResults, List<ScriptJmxNode> scriptJmxNodes) {
        if (CollectionUtils.isNotEmpty(sceneLinkRelateResults)) {
            Map<String, String> xpathMd5Map = sceneLinkRelateResults.stream().filter(o -> StringUtils.isNotBlank(o.getScriptXpathMd5()))
                    .collect(Collectors.toMap(SceneLinkRelateResult::getScriptXpathMd5, SceneLinkRelateResult::getBusinessLinkId));
            List<Long> businessLinkIds = sceneLinkRelateResults.stream().map(o -> Long.parseLong(o.getBusinessLinkId())).collect(Collectors.toList());
            ActivityQueryParam activityQueryParam = new ActivityQueryParam();
            activityQueryParam.setActivityIds(businessLinkIds);
            List<ActivityListResult> activityList = activityDAO.getActivityList(activityQueryParam);
            if (CollectionUtils.isNotEmpty(activityList)) {
                Map<String, ActivityListResult> collect = activityList.stream().collect(Collectors.toMap(o -> o.getActivityId().toString(), t -> t));
                dealScriptJmxNodes(scriptJmxNodes, xpathMd5Map, collect);
            }
        }
    }

    /**
     * 填充处理业务活动信息
     *
     * @param scriptJmxNodes
     * @param xpathMd5Map
     * @param activityMap
     */
    private void dealScriptJmxNodes(List<ScriptJmxNode> scriptJmxNodes, Map<String, String> xpathMd5Map, Map<String, ActivityListResult> activityMap) {
        if (CollectionUtils.isNotEmpty(scriptJmxNodes)) {
            for (ScriptJmxNode scriptJmxNode : scriptJmxNodes) {
                if (xpathMd5Map.get(scriptJmxNode.getXpathMd5()) != null) {
                    ActivityListResult activityListResult = activityMap.get(xpathMd5Map.get(scriptJmxNode.getXpathMd5()));
                    if (activityListResult != null) {
                        scriptJmxNode.setBusinessApplicationName(activityListResult.getActivityName());
                        ActivityUtil.EntranceJoinEntity entranceJoinEntity;
                        if (BusinessTypeEnum.VIRTUAL_BUSINESS.getType().equals(activityListResult.getBusinessType())) {
                            entranceJoinEntity = ActivityUtil.covertVirtualEntrance(activityListResult.getEntrace());
                        } else {
                            entranceJoinEntity = ActivityUtil.covertEntrance(activityListResult.getEntrace());
                        }
                        scriptJmxNode.setBusinessServicePath(entranceJoinEntity.getServiceName());
                    }
                }
                if (CollectionUtils.isNotEmpty(scriptJmxNode.getChildren())) {
                    dealScriptJmxNodes(scriptJmxNode.getChildren(), xpathMd5Map, activityMap);
                }
            }
        }
    }

}


