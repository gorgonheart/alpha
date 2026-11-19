# 项目开发入口

- 开始工作先读跨项目统一入口 `D:/alphaonline/docs/README.md`（及 `docs/kb/` 对应专题、[开发入口](docs/DEVELOPMENT.md)），并检查当前工作区与 `D:/alphaonline` 的 `git status`/`git diff`。
- Python 只用 conda alpha311，可靠解释器为 `C:/Users/Administrator/miniconda3/envs/alpha311/python.exe`。
- 本项目是生产者：因子在完整输入上计算，Arrow 发布时才裁切；batch PKL 保留原始行情，pivot PKL 和 ready 契约不可丢失。
- 因子 C 优化采用独立 Python oracle 的 TDD；不得用 C 结果重建 golden 掩盖差异。新动量 V2 与旧动量、CciV3、ILLQ 的扩展分离。
- 变更完成同步更新 `D:/alphaonline/docs/DEVELOPMENT.md` 与受影响的 `D:/alphaonline/docs/kb/*.md`（及本仓 `docs/DEVELOPMENT.md`）。历史计划不是落地证据。
- 默认不运行 realtime_data.py、不重启、不删生产数据、不执行实盘；保留用户已有修改。部署需单独授权，目标平台需要重编译扩展。
