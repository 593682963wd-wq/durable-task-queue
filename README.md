# 离线持久任务队列

保留原桥接程序的消息标识去重、SQLite 持久化、领取状态、中断后未知结果隔离；补充原子领取和条件状态迁移，可作为调用方任务库。

## 运行与验证

Python 3.11+，标准库。

```sh
python3 -B -m unittest discover -s tests -v
python3 queue_core.py --db /tmp/example-queue.sqlite enqueue sample-1 '{"text":"synthetic"}'
python3 queue_core.py --db /tmp/example-queue.sqlite claim
python3 queue_core.py --db /tmp/example-queue.sqlite recover
python3 queue_core.py --db /tmp/example-queue.sqlite get sample-1
```

数据库可能含调用者的私人任务，保存在源码目录外。

## 拆分边界

仅保留队列组件。发送器、平台 SDK、钥匙串、身份白名单、聊天文本样例、桌面控制、剪贴板操作和账号配置全部剥离。恢复须在消费者停止后显式调用；未知任务不会自动重发。

初始数据均为合成样例。运行后用户输入/导出属于用户自己的数据，不能随源码发布。源码采用 MIT 许可证，见 LICENSE；本目录没有携带原仓库历史。

`claim()` 返回单调递增的 `attempt`。所有 `transition` 必须带同一领取代次；同一个任务重试后，旧消费者不能关闭新一代任务。CLI示例：`python3 queue_core.py --db /tmp/example-queue.sqlite transition sample-1 sending done 1`。队列只管理状态；接入实际外部执行时仍须由适配器处理外部副作用的幂等性。

## 2026-11-08 候选修订

- 迁移检查与ALTER处于同一BEGIN IMMEDIATE事务；失败关闭连接。

使用包内合成样例复跑；发布集合根目录的 `verify_release.py --project external-durable-queue` 会依次执行测试和首次使用命令。原有版本的测试计数属于当时记录，本轮汇总以集合验证报告为准。
