from datetime import datetime
import uuid
from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import time
import time

vj = {
  "_index": "safe:sec-edr-sone-new-2024.11.07",
  "_type": "_doc",
  "_id": "dCDgBZMBOWm8qxDumIXD",
  "_version": 1,
  "_score": "null",
  "fields": {
    "event.original.trace_id.keyword": [
      "0BEC93B6-7679-42E7-9DAB-D4A8328D789B"
    ],
    "event.original.tgt_process_cmdline": [
      "logger -p security.warning audit warning: closefile /var/audit/20241107090459.20241107090459"
    ],
    "tgt_process_cmdline": [
      "logger -p security.warning audit warning: closefile /var/audit/20241107090459.20241107090459"
    ],
    "src_process_image_path": [
      "/bin/bash"
    ],
    "event.original.tgt_process_uid": [
      "A9C24DA5-2DD4-43FE-B9BB-DE3BFCB5B53D"
    ],
    "i_scheme.keyword": [
      "edr"
    ],
    "group_id.keyword": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "tgt_process_publisher.keyword": [
      "<Type=Apple/ID=com.apple.logger>"
    ],
    "event.original.src_process_verifiedStatus.keyword": [
      "verified"
    ],
    "event.original.tgt_process_startTime": [
      "1730970299944"
    ],
    "event.original.src_process_sessionId.keyword": [
      "0"
    ],
    "event.original.tgt_process_integrityLevel.keyword": [
      "INTEGRITY_LEVEL_UNKNOWN"
    ],
    "tgt_process_storyline_id": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "event.original.src_process_image_md5.keyword": [
      "a53c5ea2fd0425c14a765d16195014eb"
    ],
    "endpoint_name": [
      "SG00790ML’s MacBook Pro"
    ],
    "event.original.account_id.keyword": [
      "1428013551617363473"
    ],
    "event.original.tgt_process_signedStatus.keyword": [
      "signed"
    ],
    "mgmt_osRevision.keyword": [
      "15.0.1 (24A348)"
    ],
    "event.original.src_process_name.keyword": [
      "bash"
    ],
    "event.original.meta_event_name": [
      "PROCESSCREATION"
    ],
    "src_process_publisher": [
      "<Type=Apple/ID=com.apple.bash>"
    ],
    "event.original.tgt_process_image_sha1": [
      "f24270dfa1a27fdf6107cc35ce86618437cb877a"
    ],
    "event.original.src_process_image_sha1.keyword": [
      "ab79f4bf3900a7402921cc70a9fa5574f32835ac"
    ],
    "tgt_process_name": [
      "logger"
    ],
    "site_id": [
      "1428013552405892636"
    ],
    "account_id.keyword": [
      "1428013551617363473"
    ],
    "event.original.i_version": [
      "preprocess-lib-1.0"
    ],
    "event.original.src_process_storyline_id.keyword": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "event.original.src_process_cmdline": [
      "/bin/sh /etc/security/audit_warn closefile /var/audit/20241107090459.20241107090459"
    ],
    "event.original.src_process_integrityLevel.keyword": [
      "INTEGRITY_LEVEL_UNKNOWN"
    ],
    "event.original.src_process_image_path": [
      "/bin/bash"
    ],
    "event.original.src_process_image_path.keyword": [
      "/bin/bash"
    ],
    "src_process_uid.keyword": [
      "17DED8D6-98B1-48C3-B70D-BB95C6C77626"
    ],
    "tgt_process_isNative64Bit": [
      "false"
    ],
    "process_unique_key.keyword": [
      "A9C24DA5-2DD4-43FE-B9BB-DE3BFCB5B53D"
    ],
    "event.original.src_process_storyline_id": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "event.original.endpoint_type": [
      "laptop"
    ],
    "tgt_process_displayName.keyword": [
      "logger"
    ],
    "event.original.src_process_image_sha256.keyword": [
      "6ba86f6580b43493afd6f37f0aaf3cfc51bbca0ec10f12f3602ddc41330acfee"
    ],
    "event.original.tgt_process_verifiedStatus.keyword": [
      "verified"
    ],
    "event.original.mgmt_osRevision.keyword": [
      "15.0.1 (24A348)"
    ],
    "src_process_sessionId": [
      "0"
    ],
    "endpoint_type.keyword": [
      "laptop"
    ],
    "account_name": [
      "Wechain Limited"
    ],
    "tgt_process_name.keyword": [
      "logger"
    ],
    "tgt_file_isSigned": [
      "signed"
    ],
    "timestamp": [
      "2024-11-07T09:04:59.944Z"
    ],
    "event.original.src_process_cmdline.keyword": [
      "/bin/sh /etc/security/audit_warn closefile /var/audit/20241107090459.20241107090459"
    ],
    "event.original.os_name.keyword": [
      "macOS"
    ],
    "event.original.src_process_uid.keyword": [
      "17DED8D6-98B1-48C3-B70D-BB95C6C77626"
    ],
    "src_process_user": [
      "root"
    ],
    "event.original.tgt_process_sessionId.keyword": [
      "0"
    ],
    "event.original.i_scheme": [
      "edr"
    ],
    "event.original.tgt_process_user": [
      "root"
    ],
    "event.original.dataSource_vendor.keyword": [
      "SentinelOne"
    ],
    "tgt_process_storyline_id.keyword": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "i_version.keyword": [
      "preprocess-lib-1.0"
    ],
    "event_id": [
      "0BEC93B6-7679-42E7-9DAB-D4A8328D789B_577"
    ],
    "account_id": [
      "1428013551617363473"
    ],
    "event.original.mgmt_url": [
      "apne1-1002.sentinelone.net"
    ],
    "event.original.src_process_isNative64Bit": [
      "false"
    ],
    "event.original.agent_uuid": [
      "251BDF45-2F52-5658-886D-CE13446ED988"
    ],
    "event_id.keyword": [
      "0BEC93B6-7679-42E7-9DAB-D4A8328D789B_577"
    ],
    "tgt_process_publisher": [
      "<Type=Apple/ID=com.apple.logger>"
    ],
    "event.original.tgt_process_image_path.keyword": [
      "/usr/bin/logger"
    ],
    "src_process_image_md5.keyword": [
      "a53c5ea2fd0425c14a765d16195014eb"
    ],
    "endpoint_os": [
      "osx"
    ],
    "event.original.tgt_process_integrityLevel": [
      "INTEGRITY_LEVEL_UNKNOWN"
    ],
    "event.original.endpoint_os.keyword": [
      "osx"
    ],
    "event.original.src_process_verifiedStatus": [
      "verified"
    ],
    "event.original.src_process_signedStatus": [
      "signed"
    ],
    "event.original.group_id": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "os_name.keyword": [
      "macOS"
    ],
    "src_process_image_sha256": [
      "6ba86f6580b43493afd6f37f0aaf3cfc51bbca0ec10f12f3602ddc41330acfee"
    ],
    "i_version": [
      "preprocess-lib-1.0"
    ],
    "event.original.dataSource_category": [
      "security"
    ],
    "event.original.event_category.keyword": [
      "process"
    ],
    "src_process_publisher.keyword": [
      "<Type=Apple/ID=com.apple.bash>"
    ],
    "i_scheme": [
      "edr"
    ],
    "event.original.tgt_file_isSigned.keyword": [
      "signed"
    ],
    "src_process_displayName.keyword": [
      "bash"
    ],
    "src_process_user.keyword": [
      "root"
    ],
    "event.original.tgt_process_uid.keyword": [
      "A9C24DA5-2DD4-43FE-B9BB-DE3BFCB5B53D"
    ],
    "src_process_signedStatus.keyword": [
      "signed"
    ],
    "event.original.agent_uuid.keyword": [
      "251BDF45-2F52-5658-886D-CE13446ED988"
    ],
    "endpoint_name.keyword": [
      "SG00790ML’s MacBook Pro"
    ],
    "tgt_process_startTime": [
      "1730970299944"
    ],
    "src_process_uid": [
      "17DED8D6-98B1-48C3-B70D-BB95C6C77626"
    ],
    "dataSource_category": [
      "security"
    ],
    "mgmt_osRevision": [
      "15.0.1 (24A348)"
    ],
    "event.original.process_unique_key": [
      "A9C24DA5-2DD4-43FE-B9BB-DE3BFCB5B53D"
    ],
    "site_name": [
      "Default site"
    ],
    "src_process_pid.keyword": [
      "29611"
    ],
    "src_process_signedStatus": [
      "signed"
    ],
    "event.original.tgt_process_signedStatus": [
      "signed"
    ],
    "src_process_storyline_id.keyword": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "src_process_name": [
      "bash"
    ],
    "tgt_process_image_sha1.keyword": [
      "f24270dfa1a27fdf6107cc35ce86618437cb877a"
    ],
    "tgt_process_image_path": [
      "/usr/bin/logger"
    ],
    "agent_uuid.keyword": [
      "251BDF45-2F52-5658-886D-CE13446ED988"
    ],
    "mgmt_id.keyword": [
      "7096"
    ],
    "event.original.agent_version.keyword": [
      "24.2.2.7632"
    ],
    "dataSource_category.keyword": [
      "security"
    ],
    "dataSource_name.keyword": [
      "SentinelOne"
    ],
    "tgt_process_image_path.keyword": [
      "/usr/bin/logger"
    ],
    "event.original.tgt_process_image_sha256": [
      "e4e57d8e553042a9bc4d0bc932ebe09589aee03292ed4dca052dfa8cb1c0f899"
    ],
    "event.original.src_process_startTime": [
      "1730970299920"
    ],
    "tgt_process_pid": [
      "29612"
    ],
    "event.original.src_process_pid": [
      "29611"
    ],
    "event.original.event_id.keyword": [
      "0BEC93B6-7679-42E7-9DAB-D4A8328D789B_577"
    ],
    "event.original.src_process_startTime.keyword": [
      "1730970299920"
    ],
    "event.original.os_name": [
      "macOS"
    ],
    "event.original.tgt_process_pid.keyword": [
      "29612"
    ],
    "event.original.src_process_subsystem.keyword": [
      "SUBSYSTEM_UNKNOWN"
    ],
    "mgmt_url": [
      "apne1-1002.sentinelone.net"
    ],
    "tgt_process_displayName": [
      "logger"
    ],
    "event.original.endpoint_os": [
      "osx"
    ],
    "event.original.tgt_process_storyline_id": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "tgt_process_isRedirectCmdProcessor": [
      "false"
    ],
    "event_category": [
      "process"
    ],
    "src_process_integrityLevel": [
      "INTEGRITY_LEVEL_UNKNOWN"
    ],
    "dataSource_name": [
      "SentinelOne"
    ],
    "tgt_process_verifiedStatus": [
      "verified"
    ],
    "tgt_process_image_sha256.keyword": [
      "e4e57d8e553042a9bc4d0bc932ebe09589aee03292ed4dca052dfa8cb1c0f899"
    ],
    "src_process_isRedirectCmdProcessor": [
      "false"
    ],
    "event.original.mgmt_id": [
      "7096"
    ],
    "event.original.group_id.keyword": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "group_id": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "event.original.src_process_publisher.keyword": [
      "<Type=Apple/ID=com.apple.bash>"
    ],
    "event.original.tgt_file_isSigned": [
      "signed"
    ],
    "event.original.event_id": [
      "0BEC93B6-7679-42E7-9DAB-D4A8328D789B_577"
    ],
    "tgt_process_user.keyword": [
      "root"
    ],
    "event.original.packet_id": [
      "6E585678-580C-4E58-B35C-18B23C2139A6"
    ],
    "event.original.dataSource_category.keyword": [
      "security"
    ],
    "src_process_displayName": [
      "bash"
    ],
    "event.original.tgt_process_storyline_id.keyword": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "src_process_integrityLevel.keyword": [
      "INTEGRITY_LEVEL_UNKNOWN"
    ],
    "src_process_storyline_id": [
      "EE0C6F4A-AE9C-4062-BACC-ED49D9E3C4A6"
    ],
    "event.original.i_version.keyword": [
      "preprocess-lib-1.0"
    ],
    "src_process_image_md5": [
      "a53c5ea2fd0425c14a765d16195014eb"
    ],
    "event.original.tgt_process_name.keyword": [
      "logger"
    ],
    "src_process_image_sha256.keyword": [
      "6ba86f6580b43493afd6f37f0aaf3cfc51bbca0ec10f12f3602ddc41330acfee"
    ],
    "event.original.tgt_process_image_path": [
      "/usr/bin/logger"
    ],
    "event.original.src_process_uid": [
      "17DED8D6-98B1-48C3-B70D-BB95C6C77626"
    ],
    "event_type": [
      "Process Creation"
    ],
    "tgt_process_subsystem": [
      "SUBSYSTEM_UNKNOWN"
    ],
    "event.original.tgt_process_isStorylineRoot": [
      "false"
    ],
    "src_process_verifiedStatus.keyword": [
      "verified"
    ],
    "event.original.site_name": [
      "Default site"
    ],
    "event.original.src_process_subsystem": [
      "SUBSYSTEM_UNKNOWN"
    ],
    "src_process_image_sha1.keyword": [
      "ab79f4bf3900a7402921cc70a9fa5574f32835ac"
    ],
    "event.original.src_process_pid.keyword": [
      "29611"
    ],
    "event.original.account_id": [
      "1428013551617363473"
    ],
    "event.original.site_id.keyword": [
      "1428013552405892636"
    ],
    "event.original.tgt_process_name": [
      "logger"
    ],
    "event.original.event_time": [
      "1730970299944"
    ],
    "process_unique_key": [
      "A9C24DA5-2DD4-43FE-B9BB-DE3BFCB5B53D"
    ],
    "agent_uuid": [
      "251BDF45-2F52-5658-886D-CE13446ED988"
    ],
    "event_category.keyword": [
      "process"
    ],
    "event.original.tgt_process_cmdline.keyword": [
      "logger -p security.warning audit warning: closefile /var/audit/20241107090459.20241107090459"
    ],
    "meta_event_name.keyword": [
      "PROCESSCREATION"
    ],
    "event.original.tgt_process_sessionId": [
      "0"
    ],
    "event.original.endpoint_name.keyword": [
      "SG00790ML’s MacBook Pro"
    ],
    "event.original.site_name.keyword": [
      "Default site"
    ],
    "tgt_file_isSigned.keyword": [
      "signed"
    ],
    "tgt_process_image_md5.keyword": [
      "6d5607ab710518ad81690c278fefeda4"
    ],
    "event.original.meta_event_name.keyword": [
      "PROCESSCREATION"
    ],
    "event.original.tgt_process_image_sha1.keyword": [
      "f24270dfa1a27fdf6107cc35ce86618437cb877a"
    ],
    "tgt_process_sessionId.keyword": [
      "0"
    ],
    "event.original.endpoint_name": [
      "SG00790ML’s MacBook Pro"
    ],
    "event.original.event_type.keyword": [
      "Process Creation"
    ],
    "event.original.i_scheme.keyword": [
      "edr"
    ],
    "event.original.tgt_process_isNative64Bit": [
      "false"
    ],
    "event.original.event_time.keyword": [
      "1730970299944"
    ],
    "src_process_subsystem.keyword": [
      "SUBSYSTEM_UNKNOWN"
    ],
    "event.original.dataSource_vendor": [
      "SentinelOne"
    ],
    "event.original.mgmt_url.keyword": [
      "apne1-1002.sentinelone.net"
    ],
    "agent_version.keyword": [
      "24.2.2.7632"
    ],
    "tgt_process_subsystem.keyword": [
      "SUBSYSTEM_UNKNOWN"
    ],
    "event.original.event_category": [
      "process"
    ],
    "event.original.src_process_image_sha256": [
      "6ba86f6580b43493afd6f37f0aaf3cfc51bbca0ec10f12f3602ddc41330acfee"
    ],
    "event.original.src_process_name": [
      "bash"
    ],
    "event.original.process_unique_key.keyword": [
      "A9C24DA5-2DD4-43FE-B9BB-DE3BFCB5B53D"
    ],
    "event.original.src_process_signedStatus.keyword": [
      "signed"
    ],
    "mgmt_id": [
      "7096"
    ],
    "src_process_subsystem": [
      "SUBSYSTEM_UNKNOWN"
    ],
    "event.original.src_process_image_sha1": [
      "ab79f4bf3900a7402921cc70a9fa5574f32835ac"
    ],
    "event.original.tgt_process_user.keyword": [
      "root"
    ],
    "event.original.src_process_displayName": [
      "bash"
    ],
    "src_process_startTime": [
      "1730970299920"
    ],
    "tgt_process_integrityLevel.keyword": [
      "INTEGRITY_LEVEL_UNKNOWN"
    ],
    "trace_id.keyword": [
      "0BEC93B6-7679-42E7-9DAB-D4A8328D789B"
    ],
    "event.original.trace_id": [
      "0BEC93B6-7679-42E7-9DAB-D4A8328D789B"
    ],
    "src_process_startTime.keyword": [
      "1730970299920"
    ],
    "endpoint_type": [
      "laptop"
    ],
    "@timestamp": [
      "2024-11-07T09:06:55.430Z"
    ],
    "tgt_process_startTime.keyword": [
      "1730970299944"
    ],
    "tgt_process_image_md5": [
      "6d5607ab710518ad81690c278fefeda4"
    ],
    "event.original.packet_id.keyword": [
      "6E585678-580C-4E58-B35C-18B23C2139A6"
    ],
    "os_name": [
      "macOS"
    ],
    "event.original.src_process_displayName.keyword": [
      "bash"
    ],
    "tgt_process_image_sha256": [
      "e4e57d8e553042a9bc4d0bc932ebe09589aee03292ed4dca052dfa8cb1c0f899"
    ],
    "event.original.src_process_image_md5": [
      "a53c5ea2fd0425c14a765d16195014eb"
    ],
    "src_process_cmdline.keyword": [
      "/bin/sh /etc/security/audit_warn closefile /var/audit/20241107090459.20241107090459"
    ],
    "tgt_process_signedStatus.keyword": [
      "signed"
    ],
    "event.original.src_process_integrityLevel": [
      "INTEGRITY_LEVEL_UNKNOWN"
    ],
    "src_process_sessionId.keyword": [
      "0"
    ],
    "tgt_process_sessionId": [
      "0"
    ],
    "endpoint_os.keyword": [
      "osx"
    ],
    "event.original.src_process_user": [
      "root"
    ],
    "src_process_image_path.keyword": [
      "/bin/bash"
    ],
    "event.original.tgt_process_verifiedStatus": [
      "verified"
    ],
    "packet_id": [
      "6E585678-580C-4E58-B35C-18B23C2139A6"
    ],
    "tgt_process_signedStatus": [
      "signed"
    ],
    "tgt_process_isStorylineRoot": [
      "false"
    ],
    "src_process_isNative64Bit": [
      "false"
    ],
    "tgt_process_image_sha1": [
      "f24270dfa1a27fdf6107cc35ce86618437cb877a"
    ],
    "event.original.account_name.keyword": [
      "Wechain Limited"
    ],
    "event.original.src_process_isStorylineRoot": [
      "false"
    ],
    "src_process_verifiedStatus": [
      "verified"
    ],
    "event.original.event_type": [
      "Process Creation"
    ],
    "site_name.keyword": [
      "Default site"
    ],
    "event.original.src_process_isRedirectCmdProcessor": [
      "false"
    ],
    "event.original.tgt_process_displayName.keyword": [
      "logger"
    ],
    "tgt_process_uid": [
      "A9C24DA5-2DD4-43FE-B9BB-DE3BFCB5B53D"
    ],
    "meta_event_name": [
      "PROCESSCREATION"
    ],
    "tgt_process_integrityLevel": [
      "INTEGRITY_LEVEL_UNKNOWN"
    ],
    "site_id.keyword": [
      "1428013552405892636"
    ],
    "event.original.tgt_process_isRedirectCmdProcessor": [
      "false"
    ],
    "event.original.src_process_publisher": [
      "<Type=Apple/ID=com.apple.bash>"
    ],
    "src_process_name.keyword": [
      "bash"
    ],
    "event.original.tgt_process_image_sha256.keyword": [
      "e4e57d8e553042a9bc4d0bc932ebe09589aee03292ed4dca052dfa8cb1c0f899"
    ],
    "src_process_isStorylineRoot": [
      "false"
    ],
    "event.original.tgt_process_pid": [
      "29612"
    ],
    "tgt_process_verifiedStatus.keyword": [
      "verified"
    ],
    "event.original.site_id": [
      "1428013552405892636"
    ],
    "event.original.dataSource_name": [
      "SentinelOne"
    ],
    "event.original.src_process_user.keyword": [
      "root"
    ],
    "event_type.keyword": [
      "Process Creation"
    ],
    "mgmt_url.keyword": [
      "apne1-1002.sentinelone.net"
    ],
    "dataSource_vendor": [
      "SentinelOne"
    ],
    "event.original.mgmt_osRevision": [
      "15.0.1 (24A348)"
    ],
    "packet_id.keyword": [
      "6E585678-580C-4E58-B35C-18B23C2139A6"
    ],
    "event.original.tgt_process_startTime.keyword": [
      "1730970299944"
    ],
    "tgt_process_cmdline.keyword": [
      "logger -p security.warning audit warning: closefile /var/audit/20241107090459.20241107090459"
    ],
    "src_process_cmdline": [
      "/bin/sh /etc/security/audit_warn closefile /var/audit/20241107090459.20241107090459"
    ],
    "event.original.endpoint_type.keyword": [
      "laptop"
    ],
    "account_name.keyword": [
      "Wechain Limited"
    ],
    "tgt_process_pid.keyword": [
      "29612"
    ],
    "event.original.tgt_process_displayName": [
      "logger"
    ],
    "event.original.dataSource_name.keyword": [
      "SentinelOne"
    ],
    "tgt_process_uid.keyword": [
      "A9C24DA5-2DD4-43FE-B9BB-DE3BFCB5B53D"
    ],
    "trace_id": [
      "0BEC93B6-7679-42E7-9DAB-D4A8328D789B"
    ],
    "src_process_pid": [
      "29611"
    ],
    "event.original.tgt_process_image_md5.keyword": [
      "6d5607ab710518ad81690c278fefeda4"
    ],
    "tgt_process_user": [
      "root"
    ],
    "event.original.mgmt_id.keyword": [
      "7096"
    ],
    "event.original.tgt_process_publisher.keyword": [
      "<Type=Apple/ID=com.apple.logger>"
    ],
    "dataSource_vendor.keyword": [
      "SentinelOne"
    ],
    "event.original.tgt_process_subsystem": [
      "SUBSYSTEM_UNKNOWN"
    ],
    "src_process_image_sha1": [
      "ab79f4bf3900a7402921cc70a9fa5574f32835ac"
    ],
    "event.original.src_process_sessionId": [
      "0"
    ],
    "event.original.tgt_process_publisher": [
      "<Type=Apple/ID=com.apple.logger>"
    ],
    "agent_version": [
      "24.2.2.7632"
    ],
    "event.original.account_name": [
      "Wechain Limited"
    ],
    "event.original.agent_version": [
      "24.2.2.7632"
    ],
    "event_time": [
      "2024-11-07T09:04:59.944Z"
    ],
    "event.original.tgt_process_subsystem.keyword": [
      "SUBSYSTEM_UNKNOWN"
    ],
    "event.original.tgt_process_image_md5": [
      "6d5607ab710518ad81690c278fefeda4"
    ]
  },
  "sort": [
    1730970299944
  ]
}
# Define the MSK Serverless bootstrap servers and topic
bootstrap_servers = 'b-1.t3smallcluster.61x4bt.c6.kafka.us-west-2.amazonaws.com:9092'
topic = 'mytopic5'

# Create a token provider for MSK IAM authentication
class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-west-2')
        return token

# Configure the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send a message to the Kafka topic
def send_message(message):
    future = producer.send(topic, message)
    try:
        record_metadata = future.get(timeout=10)
        print(f"Message sent successfully to {record_metadata.topic} [{record_metadata.partition}] @ offset {record_metadata.offset}")
    except Exception as e:
        print(f"Error sending message: {str(e)}")
        
import random
import string

def generate_random_string(size_bytes):
    # Generate a random string of ASCII letters and digits
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size_bytes))
  
def message_gen(input):

    current_time = datetime.now()
    input["fields"]["event_time"][0] = current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    uuid_str=str(uuid.uuid4())
    print(input["fields"]["event_time"][0])
    input['fields']['agent_uuid'][0] = uuid_str
    print(input['fields']['agent_uuid'][0])
    input['fields']['src_process_cmdline'][0] = generate_random_string(655)
    return input
# Example usage
if __name__ == "__main__":
    while True:
        msg = message_gen(vj)
        send_message(msg)
        print("Message sent successfully")
        time.sleep(3)

# Close the producer when done
producer.close()