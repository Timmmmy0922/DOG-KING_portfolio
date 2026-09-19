# Python 自动化项目集

这个仓库包含三个可独立运行的 Python 自动化脚本：

| 脚本 | 主要用途 | 核心技术 |
| --- | --- | --- |
| `xianyu_Crawler.py` | 使用浏览器采集贴吧帖子并下载原图 | Selenium、requests、Cookie 会话同步 |
| `selenium百度貼吧.py` | 分离列表采集与详情渲染，并输出下载统计 | requests、BeautifulSoup、Selenium、pandas |
| `微信家教订单监控脚本.py` | 监控群消息、筛选订单、计算通勤并推送通知 | 轮询、正则解析、双地图容灾、并发、缓存、飞书 Webhook |

> `xianyu_Crawler.py` 是早期保留的文件名，当前实现采集的是百度贴吧，不是闲鱼。面试或继续维护时应主动说明这个命名历史。

## 安装依赖

```powershell
python -m pip install -r requirements.txt
```

Chrome 与 Selenium 使用的 ChromeDriver 版本需要兼容。新版 Selenium 通常会通过 Selenium Manager 自动处理驱动。

## 配置原则

账号 Cookie、API Key、Webhook、群 ID、家庭地址和输出目录都不写入源代码。请参考 [.env.example](.env.example) 中的变量名，在操作系统中设置环境变量；脚本不会自动读取 `.env` 文件。

例如，在当前 PowerShell 会话中配置贴吧爬虫：

```powershell
$env:BAIDU_COOKIE = '<从自己的浏览器会话中取得>'
$env:IMAGE_OUTPUT_DIR = '.\images'
python .\selenium百度貼吧.py
```

未配置 `BAIDU_COOKIE` 时，两个贴吧脚本会尝试使用匿名会话。微信监控脚本会在启动时检查必要配置，缺失时直接报错。

## 面试准备

完整的项目流程、设计理由、异常场景和常见追问见 [面试讲解.md](面试讲解.md)。

## 安全提醒

- 不要提交 `.env`、Cookie、个人 token、运行日志或缓存文件。
- 如果凭证曾经提交到 Git，仅删除当前文件还不够；应先撤销旧凭证，再根据仓库发布情况决定是否重写 Git 历史。
- 爬取公开页面前应确认网站条款、访问频率和数据使用方式符合规定。
