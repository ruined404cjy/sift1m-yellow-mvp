# GitHub 到 CodeHub 的源码与数据同步

## 1. 仓库职责

GitHub `main` 是源码上游，只保存源码、配置和 `downloads/.gitkeep`。CodeHub `main`
合并 GitHub 更新，并额外保存 `downloads/` 下的数据集 Git LFS pointer；数据对象上传到
CodeHub LFS。同步方向固定为 GitHub 到 CodeHub，CodeHub 数据提交不进入 GitHub。

两个 remote 共用版本化的 `.gitignore` 和 `.gitattributes`。公共配置一致可以让后续
GitHub 源码更新直接合并到 CodeHub。remote 地址、默认推送目标和 LFS endpoint 属于
黄区工作区配置，只写入 `.git/config`。

## 2. 公共版本化配置

`.gitignore` 忽略 `downloads/` 下的未跟踪文件并保留目录占位文件：

```gitignore
/downloads/**
!/downloads/.gitkeep
```

`.gitattributes` 对任意被提交的数据集文件应用 LFS，不依赖数据集名称、目录层次或
文件后缀：

```gitattributes
downloads/** filter=lfs diff=lfs merge=lfs -text
downloads/.gitkeep !filter !diff !merge text
```

仓库不提交 `.lfsconfig`，也不设置仓库级 `lfs.url`。Git LFS 默认根据命令使用的
remote 推导 endpoint。CodeHub 使用独立 endpoint 时，在黄区本地配置
`remote.codehub.lfsurl` 和 `remote.codehub.lfspushurl`。

## 3. 黄区 remote 配置

remote 名称固定为 `github` 和 `codehub`。CodeHub 是黄区 `main` 的跟踪和默认推送
目标：

```bash
git remote -v
git config --local remote.pushDefault codehub
git config --local remote.lfsdefault codehub
git config --local remote.lfspushdefault codehub
git config --local branch.main.remote codehub
git config --local branch.main.merge refs/heads/main
```

CodeHub 无法从 Git remote URL 推导 LFS endpoint 时增加：

```bash
git config --local remote.codehub.lfsurl '<CODEHUB_LFS_ENDPOINT>'
git config --local remote.codehub.lfspushurl '<CODEHUB_LFS_ENDPOINT>'
```

黄区所有推送均显式指定 `codehub`。`github` remote 用于获取源码更新。

## 4. 合并 GitHub 更新

在跟踪 CodeHub `main` 的黄区分支执行：

```bash
git status --short --branch
git fetch github main
git merge --no-ff github/main
git lfs fsck
git push codehub main
```

首次采用本约定时，CodeHub 可能已有不同的 `.gitignore` 或 `.gitattributes`。首次合并
统一采用 GitHub 中的公共版本；已跟踪的 SIFT 和 GIST LFS 文件不受 `.gitignore`
影响。合并完成后两端配置保持一致，后续源码同步不再产生配置差异。

合并前工作区必须干净。合并结果只推送到 CodeHub，不将 CodeHub `main` 推送到
GitHub。GitHub 更新不包含数据 pointer，因此无需从 GitHub LFS 拉取数据。

## 5. 新增数据集

将下载并校验通过的数据文件放入 `downloads/`。公共 `.gitignore` 提供误提交门禁，
CodeHub 数据提交对明确路径使用 `git add -f`：

```bash
git check-attr filter diff merge -- downloads/<dataset>/<file>
git add -f -- downloads/<dataset>/<file>
git lfs status
git commit -m "add <dataset> dataset"
git push codehub main
```

`git check-attr` 的期望结果为 `filter: lfs`、`diff: lfs`、`merge: lfs`。`git push`
触发 Git LFS pre-push hook，将对象上传到 CodeHub LFS。新增数据集无需执行
`git lfs track`，也无需修改 `.gitignore` 或 `.gitattributes`。

提交多个文件时逐个列出已校验的稳定文件，避免将下载缓存、临时文件和解压中间产物
加入提交。提交完成后执行：

```bash
git lfs ls-files
git lfs fsck
```

已有 SIFT、GIST 文件确认为 LFS pointer 后不执行 `git lfs migrate import`；该命令会
重写历史。
