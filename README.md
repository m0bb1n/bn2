# Control Panel v2

## Master Configurations


Store configurations in a json file in the following format
```json
[
  {"key": "master_ip", "value": "0.0.0.0"},
  {"key": "is_only_lan", "value": false}
]
```
|Key|Description|type|default|required|
|----------------|---------|----------------------|-----------------------------|----
|`master_ip`|IP for master server. If `is_only_lan` is false set it to public IP/domain or `0.0.0.0`|`str`|`127.0.0.1`| yes
|`master_port`|Port for master server|`int`|`5600`| yes
|`is_only_lan`|Only accepts connections from LAN|`bool`|`true`| yes
|`master_db_engine` | DB engine type (ie mysql, sqlite3) |`str`|`mysql`| yes
|`master_db_address`|Database address or file path (if sqlite3)| `str`|`null`|yes
> More configs are available for edit in control panel.  Go to bots>master tab to view full master configs.
