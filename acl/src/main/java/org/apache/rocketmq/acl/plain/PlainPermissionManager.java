/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.acl.plain;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.srvutil.FileWatchService;

public class PlainPermissionManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    // ACL默认配置文件名称，默认值为：/conf/plain_acl.yml
    private static final String DEFAULT_PLAIN_ACL_FILE = "/conf/plain_acl.yml";

    // 配置文件所在的目录，默认为RocketMQ的主目录
    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
            System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    // ACL配置文件名称
    private String fileName = System.getProperty("rocketmq.acl.plain.file", DEFAULT_PLAIN_ACL_FILE);

    // 权限配置映射表，用户名为key。
    private Map<String/** AccessKey **/, PlainAccessResource> plainAccessResourceMap = new HashMap<>();

    private List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

    // 远程IP解析策略工厂，用于解析白名单IP地址
    private RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    // 是否兼容plain_acl.yml文件，一旦改文件的内容改变，可以在不重启服务的情况下自动生效。
    private boolean isWatchStart;

    // 配置文件版本号
    private final DataVersion dataVersion = new DataVersion();

    public PlainPermissionManager() {
        // 解析ACL配置文件，将ACL配置规则加载到内存中。
        load();
        // 监听ACL配置文件的变更
        watch();
    }

    public void load() {

        Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
        List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

        // 第一步：通过YAML类库将配置文件解析成一个JSONObject
        JSONObject plainAclConfData = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
                JSONObject.class);
        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            throw new AclException(String.format("%s file is not data", fileHome + File.separator + fileName));
        }
        log.info("Broker plain acl conf data is : ", plainAclConfData.toString());
        /**
         * 第二步：根据ACL配置文件中配置的全局白名单列表，构建对应规则的地址校验器（RemoteAddressStrategy）
         *      1）NullRemoteAddressStrategy --> 表示全部匹配，该条规则直接返回true，将会阻断其他规则的判断，慎用
         *      2）BlankRemoteAddressStrategy --> 表示不设置白名单，该条规则默认返回false。
         *      3）MultipleRemoteAddressStrategy --> 多地址匹配模式，IP地址的最后一组使用{},大括号中可以包含多个IP地址，用英文逗号隔开，例如：192.168.0.{100,101}
         *      4）RangeRemoteAddressStrategy --> 范围类地址匹配模式，例如：192.168.*. 或 192.168.100-200.10-20
         *      5）OneRemoteAddressStrategy --> 单个IP地址配置模式，例如：192.168.1.1
         */
        JSONArray globalWhiteRemoteAddressesList = plainAclConfData.getJSONArray("globalWhiteRemoteAddresses");
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (int i = 0; i < globalWhiteRemoteAddressesList.size(); i++) {
                globalWhiteRemoteAddressStrategy.add(remoteAddressStrategyFactory.
                        getRemoteAddressStrategy(globalWhiteRemoteAddressesList.getString(i)));
            }
        }

        // 第三步：接收account的标签，按用户名将配置规则存入plainAccessResourceMap。
        JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
        if (accounts != null && !accounts.isEmpty()) {
            List<PlainAccessConfig> plainAccessConfigList = accounts.toJavaList(PlainAccessConfig.class);
            for (PlainAccessConfig plainAccessConfig : plainAccessConfigList) {
                PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
            }
        }

        // For loading dataversion part just
        JSONArray tempDataVersion = plainAclConfData.getJSONArray(AclConstants.CONFIG_DATA_VERSION);
        if (tempDataVersion != null && !tempDataVersion.isEmpty()) {
            List<DataVersion> dataVersion = tempDataVersion.toJavaList(DataVersion.class);
            DataVersion firstElement = dataVersion.get(0);
            this.dataVersion.assignNewOne(firstElement);
        }

        this.globalWhiteRemoteAddressStrategy = globalWhiteRemoteAddressStrategy;
        this.plainAccessResourceMap = plainAccessResourceMap;
    }

    public String getAclConfigDataVersion() {
        return this.dataVersion.toJson();
    }

    private Map<String, Object> updateAclConfigFileVersion(Map<String, Object> updateAclConfigMap) {

        dataVersion.nextVersion();
        List<Map<String, Object>> versionElement = new ArrayList<Map<String, Object>>();
        Map<String, Object> accountsMap = new LinkedHashMap<String, Object>() {
            {
                put(AclConstants.CONFIG_COUNTER, dataVersion.getCounter().longValue());
                put(AclConstants.CONFIG_TIME_STAMP, dataVersion.getTimestamp());
            }
        };
        versionElement.add(accountsMap);
        updateAclConfigMap.put(AclConstants.CONFIG_DATA_VERSION, versionElement);
        return updateAclConfigMap;
    }

    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {

        if (plainAccessConfig == null) {
            log.error("Parameter value plainAccessConfig is null,Please check your parameter");
            return false;
        }

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
                Map.class);

        List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
        Map<String, Object> updateAccountMap = null;
        if (accounts != null) {
            for (Map<String, Object> account : accounts) {
                if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig.getAccessKey())) {
                    // Update acl access config elements
                    accounts.remove(account);
                    updateAccountMap = createAclAccessConfigMap(account, plainAccessConfig);
                    accounts.add(updateAccountMap);
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);

                    if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                        return true;
                    }
                    return false;
                }
            }
            // Create acl access config elements
            accounts.add(createAclAccessConfigMap(null, plainAccessConfig));
            aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                return true;
            }
            return false;
        }

        log.error("Users must ensure that the acl yaml config file has accounts node element");
        return false;
    }

    private Map<String, Object> createAclAccessConfigMap(Map<String, Object> existedAccountMap, PlainAccessConfig plainAccessConfig) {

        Map<String, Object> newAccountsMap = null;
        if (existedAccountMap == null) {
            newAccountsMap = new LinkedHashMap<String, Object>();
        } else {
            newAccountsMap = existedAccountMap;
        }

        if (StringUtils.isEmpty(plainAccessConfig.getAccessKey()) ||
                plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH) {
            throw new AclException(String.format(
                    "The accessKey=%s cannot be null and length should longer than 6",
                    plainAccessConfig.getAccessKey()));
        }
        newAccountsMap.put(AclConstants.CONFIG_ACCESS_KEY, plainAccessConfig.getAccessKey());

        if (!StringUtils.isEmpty(plainAccessConfig.getSecretKey())) {
            if (plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
                throw new AclException(String.format(
                        "The secretKey=%s value length should longer than 6",
                        plainAccessConfig.getSecretKey()));
            }
            newAccountsMap.put(AclConstants.CONFIG_SECRET_KEY, (String) plainAccessConfig.getSecretKey());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getWhiteRemoteAddress())) {
            newAccountsMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig.getWhiteRemoteAddress());
        }
        if (!StringUtils.isEmpty(String.valueOf(plainAccessConfig.isAdmin()))) {
            newAccountsMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig.isAdmin());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultTopicPerm())) {
            newAccountsMap.put(AclConstants.CONFIG_DEFAULT_TOPIC_PERM, plainAccessConfig.getDefaultTopicPerm());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultGroupPerm())) {
            newAccountsMap.put(AclConstants.CONFIG_DEFAULT_GROUP_PERM, plainAccessConfig.getDefaultGroupPerm());
        }
        if (plainAccessConfig.getTopicPerms() != null && !plainAccessConfig.getTopicPerms().isEmpty()) {
            newAccountsMap.put(AclConstants.CONFIG_TOPIC_PERMS, plainAccessConfig.getTopicPerms());
        }
        if (plainAccessConfig.getGroupPerms() != null && !plainAccessConfig.getGroupPerms().isEmpty()) {
            newAccountsMap.put(AclConstants.CONFIG_GROUP_PERMS, plainAccessConfig.getGroupPerms());
        }

        return newAccountsMap;
    }

    public boolean deleteAccessConfig(String accesskey) {
        if (StringUtils.isEmpty(accesskey)) {
            log.error("Parameter value accesskey is null or empty String,Please check your parameter");
            return false;
        }

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
                Map.class);

        List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get("accounts");
        if (accounts != null) {
            Iterator<Map<String, Object>> itemIterator = accounts.iterator();
            while (itemIterator.hasNext()) {

                if (itemIterator.next().get(AclConstants.CONFIG_ACCESS_KEY).equals(accesskey)) {
                    // Delete the related acl config element
                    itemIterator.remove();
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);

                    if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                        return true;
                    }
                    return false;
                }
            }
        }
        log.error("Users must ensure that the acl yaml config file has related acl config elements");

        return false;
    }

    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {

        if (globalWhiteAddrsList == null) {
            log.error("Parameter value globalWhiteAddrsList is null,Please check your parameter");
            return false;
        }

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
                Map.class);

        List<String> globalWhiteRemoteAddrList = (List<String>) aclAccessConfigMap.get(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);

        if (globalWhiteRemoteAddrList != null) {
            globalWhiteRemoteAddrList.clear();
            globalWhiteRemoteAddrList.addAll(globalWhiteAddrsList);

            // Update globalWhiteRemoteAddr element in memeory map firstly
            aclAccessConfigMap.put(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS, globalWhiteRemoteAddrList);
            if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                return true;
            }
            return false;
        }

        log.error("Users must ensure that the acl yaml config file has globalWhiteRemoteAddresses flag firstly");
        return false;
    }

    public AclConfig getAllAclConfig() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> configs = new ArrayList<>();
        List<String> whiteAddrs = new ArrayList<>();
        JSONObject plainAclConfData = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
                JSONObject.class);
        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            throw new AclException(String.format("%s file is not data", fileHome + File.separator + fileName));
        }
        JSONArray globalWhiteAddrs = plainAclConfData.getJSONArray(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
        if (globalWhiteAddrs != null && !globalWhiteAddrs.isEmpty()) {
            whiteAddrs = globalWhiteAddrs.toJavaList(String.class);
        }
        JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
        if (accounts != null && !accounts.isEmpty()) {
            configs = accounts.toJavaList(PlainAccessConfig.class);
        }
        aclConfig.setGlobalWhiteAddrs(whiteAddrs);
        aclConfig.setPlainAccessConfigs(configs);
        return aclConfig;
    }

    /**
     * 创建一个FileWatchService线程，用于监听ACL的规则配置文件，一旦配置文件的内容发生变化，则执行FileWatchService.Listener的回调函数onChanged()，执行配置规则的重新加载
     */
    private void watch() {
        try {
            String watchFilePath = fileHome + fileName;
            FileWatchService fileWatchService = new FileWatchService(new String[]{watchFilePath}, new FileWatchService.Listener() {
                @Override
                public void onChanged(String path) {
                    log.info("The plain acl yml changed, reload the context");
                    load();
                }
            });
            fileWatchService.start();
            log.info("Succeed to start AclWatcherService");
            this.isWatchStart = true;
        } catch (Exception e) {
            log.error("Failed to start AclWatcherService", e);
        }
    }

    void checkPerm(PlainAccessResource needCheckedAccess, PlainAccessResource ownedAccess) {
        // 第六步：如果当前操作要求必须拥有管理员权限，但当前用户不是管理员角色，则抛出ACL访问异常。
        if (Permission.needAdminPerm(needCheckedAccess.getRequestCode()) && !ownedAccess.isAdmin()) {
            throw new AclException(String.format("Need admin permission for request code=%d, but accessKey=%s is not", needCheckedAccess.getRequestCode(), ownedAccess.getAccessKey()));
        }
        Map<String, Byte> needCheckedPermMap = needCheckedAccess.getResourcePermMap();
        Map<String, Byte> ownedPermMap = ownedAccess.getResourcePermMap();

        // 第七步：如果本次操作的资源无需权限验证，则直接通过。
        if (needCheckedPermMap == null) {
            // If the needCheckedPermMap is null,then return
            return;
        }

        // 如果当前操作的用户是超级管理员角色，但未设置任何访问规则，也通过验证。
        if (ownedPermMap == null && ownedAccess.isAdmin()) {
            // If the ownedPermMap is null and it is an admin user, then return
            return;
        }

        /**
         * 第八步：遍历需要的权限与用户拥有的权限并进行对比，判断是否匹配；
         *         1）如果用户没有资源的访问权限，则判断默认权限是否允许操作，如果不允许，则抛出ACLException
         */
        for (Map.Entry<String, Byte> needCheckedEntry : needCheckedPermMap.entrySet()) {
            String resource = needCheckedEntry.getKey();
            Byte neededPerm = needCheckedEntry.getValue();
            boolean isGroup = PlainAccessResource.isRetryTopic(resource);

            // 如果在用户配置的访问控制规则中找不到当前资源、或用户的访问控制规则为空。进一步判断默认用户的默认Topic、Consumer访问权限
            if (ownedPermMap == null || !ownedPermMap.containsKey(resource)) {
                // Check the default perm
                // 如果所需资源是重试topic，则使用权限控制规则的消费组默认访问权限；否者使用权限控制规则的Topic默认访问权限
                byte ownedPerm = isGroup ? ownedAccess.getDefaultGroupPerm() :
                        ownedAccess.getDefaultTopicPerm();
                if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                    throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
                }
                continue;
            }
            // 如果用户
            if (!Permission.checkPermission(neededPerm, ownedPermMap.get(resource))) {
                throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
            }
        }
    }

    void clearPermissionInfo() {
        this.plainAccessResourceMap.clear();
        this.globalWhiteRemoteAddressStrategy.clear();
    }

    public PlainAccessResource buildPlainAccessResource(PlainAccessConfig plainAccessConfig) throws AclException {
        if (plainAccessConfig.getAccessKey() == null
                || plainAccessConfig.getSecretKey() == null
                || plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH
                || plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
            throw new AclException(String.format(
                    "The accessKey=%s and secretKey=%s cannot be null and length should longer than 6",
                    plainAccessConfig.getAccessKey(), plainAccessConfig.getSecretKey()));
        }
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setAccessKey(plainAccessConfig.getAccessKey());
        plainAccessResource.setSecretKey(plainAccessConfig.getSecretKey());
        plainAccessResource.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());

        plainAccessResource.setAdmin(plainAccessConfig.isAdmin());

        plainAccessResource.setDefaultGroupPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultGroupPerm()));
        plainAccessResource.setDefaultTopicPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultTopicPerm()));

        Permission.parseResourcePerms(plainAccessResource, false, plainAccessConfig.getGroupPerms());
        Permission.parseResourcePerms(plainAccessResource, true, plainAccessConfig.getTopicPerms());

        plainAccessResource.setRemoteAddressStrategy(remoteAddressStrategyFactory.
                getRemoteAddressStrategy(plainAccessResource.getWhiteRemoteAddress()));

        return plainAccessResource;
    }

    public void validate(PlainAccessResource plainAccessResource) {

        // Check the global white remote addr
        // 第一步：使用全局白名单对客户端IP进行验证，只需要白名单规则中任意一个规则匹配即通过验证
        for (RemoteAddressStrategy remoteAddressStrategy : globalWhiteRemoteAddressStrategy) {
            if (remoteAddressStrategy.match(plainAccessResource)) {
                return;
            }
        }

        // 请求中不包含用户名
        if (plainAccessResource.getAccessKey() == null) {
            throw new AclException(String.format("No accessKey is configured"));
        }

        // 第二步：如果在Broker端的ACL配置文件中没有包含请求用户的访问控制规则，则直接抛出异常，禁止本次操作
        if (!plainAccessResourceMap.containsKey(plainAccessResource.getAccessKey())) {
            throw new AclException(String.format("No acl config for %s", plainAccessResource.getAccessKey()));
        }

        // Check the white addr for accesskey
        // 第三步：验证用户级别的配置的IP白名单规则，如果客户端IP与用户级的IP白名单匹配，则直接返回校验通过。
        PlainAccessResource ownedAccess = plainAccessResourceMap.get(plainAccessResource.getAccessKey());
        if (ownedAccess.getRemoteAddressStrategy().match(plainAccessResource)) {
            return;
        }

        // Check the signature
        // 第四步：验证签名是否一致，如果一致，签名验证通过，否则返回ACL校验异常，禁止本次操作。
        String signature = AclUtils.calSignature(plainAccessResource.getContent(), ownedAccess.getSecretKey());
        if (!signature.equals(plainAccessResource.getSignature())) {
            throw new AclException(String.format("Check signature failed for accessKey=%s", plainAccessResource.getAccessKey()));
        }
        // Check perm of each resource
        // 第五步：校验资源的访问权限
        checkPerm(plainAccessResource, ownedAccess);
    }

    public boolean isWatchStart() {
        return isWatchStart;
    }
}
