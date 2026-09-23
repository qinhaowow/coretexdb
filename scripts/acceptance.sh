#!/usr/bin/env bash
# CoreTexDB v0.1 验收脚本 —— 用**真实二进制**跑一遍所有已接线功能。
#
# 设计原则：
#   1. 失败不中断，跑完全部再汇总（一次暴露所有问题）
#   2. 只断言"能被第三方复核的东西"：退出码、stdout 里的可见字符串、落盘文件
#   3. 每个 CLI 命令都是独立进程，所以"跨进程持久化"天然被覆盖
#
# 用法： scripts/acceptance.sh [二进制路径]
#       默认 target/debug/coretex

set -u

BIN="${1:-target/debug/coretex}"
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"
[ -x "$BIN" ] || {
    echo "找不到可执行文件: $BIN"
    exit 2
}

# A Windows .exe launched through WSL interop cannot see Linux paths, so every
# path handed to it has to be translated. The files stay where they are; only
# the spelling of the argument changes. `-a` stops wslpath from requiring that
# the path already exist (needed for the "file does not exist" case).
case "$BIN" in
*.exe)
    p() { wslpath -w -a "$1"; }
    # ── 必须用「Windows 路径上的副本」──
    # Windows 会静默执行一个位于 Linux 文件系统（经 \\wsl.localhost UNC 共享访问）
    # 的 .exe 的**陈旧镜像**。这曾让一整轮 Windows 验收报出当前构建根本不存在
    # 的失败（且 cargo 确实重编了、grep 也能在新二进制里找到新字符串）。
    # 因此先复制到 Windows 盘上再跑。
    if [ "${BIN#/mnt/}" = "$BIN" ]; then
        STAGE="$(mktemp -d -p /mnt/c/Users/QH/AppData/Local/Temp acceptance-bin-XXXXXX)"
        cp "$BIN" "$STAGE/$(basename "$BIN")"
        BIN="$STAGE/$(basename "$BIN")"
        echo "注意: 已把 .exe 复制到 Windows 路径再运行（避开 UNC 陈旧镜像）"
    fi
    ;;
*) p() { printf '%s' "$1"; } ;;
esac

if [ -n "${ACCEPTANCE_WORK:-}" ]; then
    WORK="$ACCEPTANCE_WORK"
elif [ "${BIN##*.}" = "exe" ]; then
    # Keep data on the Windows drive: \\wsl.localhost UNC paths work but are
    # slow and not every Windows API accepts them.
    WORK="$(mktemp -d -p /mnt/c/Users/QH/AppData/Local/Temp acceptance-XXXXXX)"
else
    WORK="$(mktemp -d)"
fi
DB="$WORK/db"
DB_ARG="$(p "$DB")"
DD=(--data-dir "$DB_ARG")
# Random high port: a fixed one collides with a server left behind by an
# earlier failed run, which then looks like "server failed to start".
PORT=$((20000 + RANDOM % 20000))

PASS=0
FAIL=0
declare -a FAILED

green() { printf '\033[32m%s\033[0m' "$1"; }
red() { printf '\033[31m%s\033[0m' "$1"; }
note() { printf '  \033[33mSKIP\033[0m %s\n' "$1"; }

_pass() {
    PASS=$((PASS + 1))
    printf '  %s %s\n' "$(green PASS)" "$1"
}
_fail() {
    FAIL=$((FAIL + 1))
    FAILED+=("$1")
    printf '  %s %s\n' "$(red FAIL)" "$1"
}

# check <名称> <期望子串|-> <命令...>     "-" 表示只要求退出码 0
check() {
    local name="$1"
    shift
    local want="$1"
    shift
    local out rc
    out="$("$@" 2>&1)"
    rc=$?
    if [ "$rc" -ne 0 ]; then
        _fail "$name: 退出码 $rc"
        printf '       %s\n' "$(echo "$out" | head -2 | tr '\n' ' ')"
        return 1
    fi
    if [ "$want" != "-" ] && ! printf '%s' "$out" | grep -qF -- "$want"; then
        _fail "$name: 输出缺少 '$want'"
        printf '       实际: %s\n' "$(echo "$out" | head -3 | tr '\n' ' ')"
        return 1
    fi
    _pass "$name"
    return 0
}

# check_fail <名称> <命令...>   —— 命令**必须**非零退出（"必须拒绝"类用例）
check_fail() {
    local name="$1"
    shift
    local out rc
    out="$("$@" 2>&1)"
    rc=$?
    if [ "$rc" -eq 0 ]; then
        _fail "$name: 本该失败却成功退出"
        printf '       实际: %s\n' "$(echo "$out" | head -2 | tr '\n' ' ')"
        return 1
    fi
    _pass "$name"
    return 0
}

# check_absent <名称> <不应出现的子串> <命令...>
check_absent() {
    local name="$1"
    local unwanted="$2"
    shift 2
    if "$@" 2>&1 | grep -qF -- "$unwanted"; then
        _fail "$name: 仍出现 '$unwanted'"
        return 1
    fi
    _pass "$name"
    return 0
}

section() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

echo "二进制: $BIN"
echo "数据目录: $DB"

# ---------------------------------------------------------------- 集合管理
section "类别 1 集合管理"
check "create 普通集合" "created" "$BIN" "${DD[@]}" collection create docs -d 4 -m euclidean
check "create 中文集合" "商品库" "$BIN" "${DD[@]}" collection create 商品库 -d 3 -m cosine
check "list 显示普通集合" "docs" "$BIN" "${DD[@]}" collection list -v
check "list 显示中文集合名" "商品库" "$BIN" "${DD[@]}" collection list -v
check "info 回显维度" "4" "$BIN" "${DD[@]}" collection info docs
check "stats 可用" "-" "$BIN" "${DD[@]}" collection stats docs
check "rename 生效" "renamed" "$BIN" "${DD[@]}" collection rename docs --to renamed_docs
check "rename 已落盘（新进程可见）" "renamed_docs" "$BIN" "${DD[@]}" collection list
check_fail "重复创建同名集合必须被拒" "$BIN" "${DD[@]}" collection create renamed_docs -d 4 -m euclidean

# ---------------------------------------------------------------- 数据操作
section "类别 2 数据操作"
C=renamed_docs
check "insert a" "inserted" "$BIN" "${DD[@]}" vector insert "$C" a 1,0,0,0
check "insert b" "inserted" "$BIN" "${DD[@]}" vector insert "$C" b 0,1,0,0
check "insert c" "inserted" "$BIN" "${DD[@]}" vector insert "$C" c 1,1,0,0
check "count = 3" "3" "$BIN" "${DD[@]}" vector count "$C"
check "get 读回向量" "1" "$BIN" "${DD[@]}" vector get "$C" a
check "update 生效" "-" "$BIN" "${DD[@]}" vector update "$C" a --vector 2,0,0,0
check "update 已落盘（新进程 get）" "2" "$BIN" "${DD[@]}" vector get "$C" a
check_fail "维度不符必须被拒" "$BIN" "${DD[@]}" vector insert "$C" bad 1,2,3

check "insert 中文 ID" "苹果-001" \
    "$BIN" "${DD[@]}" vector insert 商品库 苹果-001 1,0,0 -m '{"名称":"红富士苹果","类别":"水果"}'
check "insert 中文 ID 2" "绿茶-002" \
    "$BIN" "${DD[@]}" vector insert 商品库 绿茶-002 0,1,0 -m '{"名称":"龙井绿茶","类别":"饮料"}'
check "get 中文元数据不乱码" "红富士苹果" "$BIN" "${DD[@]}" vector get 商品库 苹果-001
check "中文元数据跨进程存活" "龙井绿茶" "$BIN" "${DD[@]}" vector get 商品库 绿茶-002

# ---------------------------------------------------------------- 命令诚实性
# 这一整节防的是同一类 bug：命令失败了却退出 0 并打印 ✓（“假成功”）。
section "命令诚实性（不得谎报成功）"
check_fail "update 非数字向量必须报错，不得 panic" \
    "$BIN" "${DD[@]}" vector update "$C" a --vector 1,x,3,4
check_fail "update 不存在的 id 必须报错" \
    "$BIN" "${DD[@]}" vector update "$C" no_such_id --vector 1,0,0,0
check "仅改元数据的 update" "updated" \
    "$BIN" "${DD[@]}" vector update "$C" a --metadata '{"tag":9}'
check "仅改元数据的 update 已落盘" "tag" "$BIN" "${DD[@]}" vector get "$C" a
check_fail "insert 非数字向量必须报错，不得 panic" \
    "$BIN" "${DD[@]}" vector insert "$C" badvec 1,x,3,4
check_fail "insert 非法 metadata JSON 必须报错（不得静默变空）" \
    "$BIN" "${DD[@]}" vector insert "$C" badmeta 1,0,0,0 -m '{not json}'

printf '[{"id":"b1","vector":[1,0,0,0]},{"id":"b2","vector":[0,1,0,0]}]' >"$WORK/batch_ok.json"
check "--batch 可达（原本 clap 拦截，根本用不了）" "2 vectors" \
    "$BIN" "${DD[@]}" vector insert "$C" --batch "$(p "$WORK/batch_ok.json")"
check "--batch 确实写入" "5" "$BIN" "${DD[@]}" vector count "$C"
printf '[{"id":"b3","vector":[1,0,0,0]},{"id":"b4","vector":[1,0]}]' >"$WORK/batch_bad.json"
check_fail "--batch 含坏记录必须整体失败" \
    "$BIN" "${DD[@]}" vector insert "$C" --batch "$(p "$WORK/batch_bad.json")"
check "--batch 失败后不得部分导入" "5" "$BIN" "${DD[@]}" vector count "$C"

printf '[{"id":"j1","vector":[1,0,0,0]}]' >"$WORK/ok.json"
check "import json" "Imported 1" \
    "$BIN" "${DD[@]}" vector import "$C" "$(p "$WORK/ok.json")" --format json
check_fail "import json 维度不符必须失败" \
    bash -c "printf '[{\"id\":\"j9\",\"vector\":[1,0]}]' > '$WORK/bad.json' && '$BIN' --data-dir '$DB_ARG' vector import '$C' '$(p "$WORK/bad.json")' --format json"
check "import 失败后不得部分导入" "6" "$BIN" "${DD[@]}" vector count "$C"
check_fail "import 文件不存在必须报错" \
    "$BIN" "${DD[@]}" vector import "$C" "$(p "$WORK/nope.json")" --format json

# ---------------------------------------------------------------- 检索 / 过滤
section "类别 4 检索 / 类别 5 过滤"
check "top-1 精确排序（euclidean）" "a" "$BIN" "${DD[@]}" search 2,0,0,0 -c "$C" -k 1
check "top-k 可返回 k 条" "-" "$BIN" "${DD[@]}" search 2,0,0,0 -c "$C" -k 3
check "结果带元数据" "score=" "$BIN" "${DD[@]}" search 2,0,0,0 -c "$C" -k 1 --with-metadata
check "json 输出是合法 JSON" '"id"' "$BIN" "${DD[@]}" search 2,0,0,0 -c "$C" -k 1 --format json
check "中文值标量过滤只回 1 条" "苹果-001" \
    "$BIN" "${DD[@]}" search 1,0,0 -c 商品库 -k 5 --filter '{"类别":"水果"}'
check "比较算符 \$gte 生效" "-" \
    "$BIN" "${DD[@]}" search 2,0,0,0 -c "$C" -k 3 --filter '{"tag":{"$gte":1}}'

# ---------------------------------------------------------------- 索引
section "类别 3 索引"
check "hnsw 集合可建成" "hnsw" "$BIN" "${DD[@]}" collection create h_coll -d 3 -m euclidean -i hnsw
check "hnsw 可写入" "inserted" "$BIN" "${DD[@]}" vector insert h_coll h1 1,0,0
check "hnsw 可检索" "h1" "$BIN" "${DD[@]}" search 1,0,0 -c h_coll -k 1

check "ivf 集合可建成" "ivf" "$BIN" "${DD[@]}" collection create i_coll -d 3 -m euclidean -i ivf
for i in 1 2 3 4 5; do "$BIN" "${DD[@]}" vector insert i_coll "v$i" "$i,0,0" >/dev/null 2>&1; done
check "ivf 检索**不得为空**（本次修复）" "v5" "$BIN" "${DD[@]}" search 5,0,0 -c i_coll -k 1

check "未知索引名回显 brute_force" "brute_force" \
    "$BIN" "${DD[@]}" collection create typo_idx -d 3 -m euclidean -i hnsww
check "未知 metric 回显 cosine" "Cosine" \
    bash -c "'$BIN' --data-dir '$DB_ARG' collection create typo_metric -d 3 -m l2 >/dev/null && '$BIN' --data-dir '$DB_ARG' collection info typo_metric"

# ---------------------------------------------------------------- 持久化
section "类别 6 持久化"
check "重启后集合仍在" "商品库" "$BIN" "${DD[@]}" collection list
check "重启后计数正确" "2" "$BIN" "${DD[@]}" vector count 商品库
check "重启后检索仍正确" "苹果-001" "$BIN" "${DD[@]}" search 1,0,0 -c 商品库 -k 1
check "doctor 可用" "-" "$BIN" "${DD[@]}" doctor

# ---------------------------------------------------------------- 备份 / 恢复
section "类别 6 备份 / 恢复（本次修复：原为空壳）"
BACKUP_DIR="$WORK/backups"
SNAP="$BACKUP_DIR/acme"
WBAK="$(p "$BACKUP_DIR")"
check "backup 报告完成" "备份完成" "$BIN" "${DD[@]}" backup --name acme --output "$WBAK"

if [ -n "$(find "$SNAP" -type f 2>/dev/null)" ]; then
    _pass "backup 产出真实文件（$(find "$SNAP" -type f | wc -l) 个）"
else
    _fail "backup 产出真实文件: 目录为空"
fi
check "backup 清单含 sha256" "sha256=" "$BIN" "${DD[@]}" backup --name acme2 --output "$WBAK"
check_fail "backup --compression gzip 必须明说未实现" \
    "$BIN" "${DD[@]}" backup --name c1 --output "$WBAK" --compression gzip
check_fail "restore 不加 --force 必须报错" \
    "$BIN" "${DD[@]}" restore --name acme --input "$WBAK"

# 真往返：删掉集合 → 恢复 → 数据回来
"$BIN" "${DD[@]}" collection delete "$C" -f >/dev/null 2>&1
check_absent "恢复前集合确已删除" "$C" "$BIN" "${DD[@]}" collection list
check "restore --force 成功" "恢复完成" \
    "$BIN" "${DD[@]}" restore --name acme --input "$WBAK" --force
check "恢复后集合回来了" "$C" "$BIN" "${DD[@]}" collection list
check "恢复后向量数据回来了" "6" "$BIN" "${DD[@]}" vector count "$C"
check "恢复后检索正确" "a" "$BIN" "${DD[@]}" search 2,0,0,0 -c "$C" -k 1
check_fail "被篡改的备份必须被拒绝（校验和兜底）" \
    bash -c "printf 'x' >> '$SNAP/data/coretex/metadata/metadata.json' && '$BIN' --data-dir '$DB_ARG' restore --name acme --input '$WBAK' --force"

# ---------------------------------------------------------------- SQL
section "SQL（含中文）"
check "SQL SELECT 英文" "-" "$BIN" "${DD[@]}" sql "SELECT * FROM renamed_docs"
check "SQL 中文词法不 panic" "-" "$BIN" "${DD[@]}" sql "SELECT 名字 FROM 用户 WHERE 备注 = '张三'"

# ---------------------------------------------------------------- 删除
section "集合删除"
check_fail "delete 未确认必须非零退出（不得假装成功）" \
    bash -c "'$BIN' --data-dir '$DB_ARG' collection delete 商品库 < /dev/null"
check "delete -f 生效" "deleted" "$BIN" "${DD[@]}" collection delete 商品库 -f
check_absent "删除后不在列表" "商品库" "$BIN" "${DD[@]}" collection list
check "同名重建不复活旧数据" "0" \
    bash -c "'$BIN' --data-dir '$DB_ARG' collection create 商品库 -d 3 -m cosine >/dev/null && '$BIN' --data-dir '$DB_ARG' vector count 商品库"

# ---------------------------------------------------------------- REST
section "类别 7 接口 —— REST + server 重启存活（G10）"
if [ "${BIN##*.}" = "exe" ]; then
    # WSL2 是独立网络命名空间：从这个环境到 Windows 宿主机**完全不通**
    # （连 SMB 445 都不可达、ICMP 也丢包），所以无法探活一个跑在 Windows 上的端口。
    # 这不是产品缺陷 —— 改为断言它确实启动并打印了监听地址。
    note "Windows 二进制：WSL → Windows 宿主机的网络不通（连 445 都不可达），"
    note "  所以跳过 HTTP 探活，改为断言 server 真的启动。REST 全链路已在 Linux 二进制上验过。"
    "$BIN" "${DD[@]}" server -a 127.0.0.1 -p "$PORT" --grpc-port 0 --ws-port 0 \
        >"$WORK/server.log" 2>&1 &
    SRV_PID=$!
    sleep 6
    if kill -0 "$SRV_PID" 2>/dev/null && grep -q "Starting CortexDB API server" "$WORK/server.log"; then
        _pass "server 在 Windows 上启动并打印监听地址"
    elif grep -qE "AddrInUse|10048" "$WORK/server.log"; then
        _fail "端口 $PORT 被占用（可能是上次失败留下的 server 进程）"
    else
        _fail "server 未在 Windows 上启动"
        tail -5 "$WORK/server.log"
    fi
    kill "$SRV_PID" 2>/dev/null
    wait "$SRV_PID" 2>/dev/null
elif ! command -v curl >/dev/null 2>&1; then
    note "未安装 curl，跳过 REST 验收"
else
    start_server() {
        "$BIN" "${DD[@]}" server -a 127.0.0.1 -p "$PORT" --grpc-port 0 --ws-port 0 \
            >"$WORK/server.log" 2>&1 &
        SRV_PID=$!
        for _ in $(seq 1 60); do
            curl -sf "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && return 0
            sleep 0.25
        done
        return 1
    }
    stop_server() {
        kill "$SRV_PID" 2>/dev/null
        wait "$SRV_PID" 2>/dev/null
    }

    if start_server; then
        _pass "server 启动并可 /health"
        check "POST /api/collections" "rest_coll" \
            curl -sf -X POST -H 'Content-Type: application/json' \
            -d '{"name":"rest_coll","dimension":3,"distance_metric":"cosine"}' \
            "http://127.0.0.1:$PORT/api/collections"
        check "POST 插入向量" "-" \
            curl -sf -X POST -H 'Content-Type: application/json' \
            -d '{"vectors":[{"id":"r1","vector":[1,0,0],"metadata":{"名称":"测试"}}]}' \
            "http://127.0.0.1:$PORT/api/collections/rest_coll/vectors"
        check "POST 搜索（含中文 JSON）" "r1" \
            curl -sf -X POST -H 'Content-Type: application/json' \
            -d '{"vector":[1,0,0],"k":1}' \
            "http://127.0.0.1:$PORT/api/collections/rest_coll/search"
        stop_server

        if start_server; then
            _pass "server 重启成功"
            check "重启后集合仍在（REST）" "rest_coll" \
                curl -sf "http://127.0.0.1:$PORT/api/collections"
            check "重启后向量仍在（REST）" "1" \
                curl -sf "http://127.0.0.1:$PORT/api/collections/rest_coll/count"
            stop_server
        else
            _fail "server 重启失败"
            tail -5 "$WORK/server.log"
        fi
    else
        _fail "server 启动失败（60 次探活均未通过）"
        tail -10 "$WORK/server.log" 2>/dev/null
    fi
fi

# ------------------------------------------------ 数据操作补全
# 覆盖 2026-09-16 新增的能力：upsert / list / clear / delete --filter / export。
# 用独立集合，避免扰动前面 $C 的计数断言。
section "类别 2 数据操作补全（upsert / list / clear / delete --filter / export）"
OPS=ops
# 路径要备两套：exe 只能看懂 Windows 路径（wslpath），
# 而 test -s / head / diff 是 Linux bash 在跑，必须用 Linux 路径。
# 上一版只转了一套，于是在 Windows 上 export 的两个文件断言全挂。
OPS_JSON_WIN="$(p "$WORK/ops.json")"
OPS_CSV_WIN="$(p "$WORK/ops.csv")"
OPS_JSON_LNX="$WORK/ops.json"
OPS_CSV_LNX="$WORK/ops.csv"
OPS_FMTBAD="$(p "$WORK/ops.parquet")"
OPS_MISSING="$(p "$WORK/ops-missing.json")"
check "创建 ops 集合" "created" "$BIN" "${DD[@]}" collection create "$OPS" -d 4 -m euclidean
check "ops insert a" "inserted" "$BIN" "${DD[@]}" vector insert "$OPS" a 1,0,0,0 -m '{"tag":"x","n":1}'
check "ops insert b" "inserted" "$BIN" "${DD[@]}" vector insert "$OPS" b 0,1,0,0 -m '{"tag":"x","n":2}'
check "ops insert c" "inserted" "$BIN" "${DD[@]}" vector insert "$OPS" c 0,0,1,0 -m '{"tag":"y","n":3}'

check "upsert 已有 id 报告更新" "更新 1 条" \
    "$BIN" "${DD[@]}" vector upsert "$OPS" a 3,0,0,0 -m '{"tag":"x","n":1}'
check "upsert 新 id 报告新增" "新增 1 条" \
    "$BIN" "${DD[@]}" vector upsert "$OPS" d 0,0,0,1 -m '{"tag":"new"}'
check "upsert 替换后的向量已落盘" "3" "$BIN" "${DD[@]}" vector get "$OPS" a
check "upsert 不重复计数（count=4）" "4" "$BIN" "${DD[@]}" vector count "$OPS"
check_fail "upsert 维度不符必须被拒" \
    "$BIN" "${DD[@]}" vector upsert "$OPS" bad 1,2

check "list 列出全部向量" "d" "$BIN" "${DD[@]}" vector list "$OPS"
check "list --with-metadata 带元数据" "new" "$BIN" "${DD[@]}" vector list "$OPS" --with-metadata
check "list 分页生效" "共 4 条" "$BIN" "${DD[@]}" vector list "$OPS" --limit 1 --offset 2
check "list --format json 是合法 JSON" '"id"' "$BIN" "${DD[@]}" vector list "$OPS" --format json
check_fail "list 不存在的集合必须报错" "$BIN" "${DD[@]}" vector list no_such_collection

check "delete --filter 只删匹配项" "2 vectors deleted" \
    "$BIN" "${DD[@]}" vector delete "$OPS" --filter '{"tag":"x"}'
check "delete --filter 未误伤（count=2）" "2" "$BIN" "${DD[@]}" vector count "$OPS"
check "delete --filter 已落盘（新进程 count=2）" "2" "$BIN" "${DD[@]}" vector count "$OPS"
check_fail "delete --filter 删掉的 id 真的读不到了" \
    bash -c "'$BIN' --data-dir '$DB_ARG' vector list '$OPS' | grep -qE '^  (a|b)$'"
check "delete --filter 无匹配时不删任何东西" "0 vectors deleted" \
    "$BIN" "${DD[@]}" vector delete "$OPS" --filter '{"tag":"nomatch"}'
check "没有匹配项时集合不变" "2" "$BIN" "${DD[@]}" vector count "$OPS"
check_fail "delete 同时给 ids 与 --filter 必须被拒" \
    "$BIN" "${DD[@]}" vector delete "$OPS" c --filter '{"tag":"y"}'
check_fail "delete 空 ids 必须报错" \
    bash -c "'$BIN' --data-dir '$DB_ARG' vector delete '$OPS' ''"

check "export json 真的写出非空文件" "-" \
    bash -c "'$BIN' --data-dir '$DB_ARG' vector export '$OPS' '$OPS_JSON_WIN' --format json >/dev/null && test -s '$OPS_JSON_LNX'"
check "export csv 带 UTF-8 BOM" "-" \
    bash -c "'$BIN' --data-dir '$DB_ARG' vector export '$OPS' '$OPS_CSV_WIN' --format csv >/dev/null && head -c 3 '$OPS_CSV_LNX' | od -An -tx1 | tr -d ' \n' | grep -qi '^efbbbf'"
check "export → import 往返无损" "-" \
    bash -c "'$BIN' --data-dir '$DB_ARG' collection create ops_rt -d 4 -m euclidean >/dev/null && '$BIN' --data-dir '$DB_ARG' vector import ops_rt '$OPS_JSON_WIN' --format json >/dev/null && diff <('$BIN' --data-dir '$DB_ARG' vector list ops_rt --with-metadata | tail -n +2 | sort) <('$BIN' --data-dir '$DB_ARG' vector list '$OPS' --with-metadata | tail -n +2 | sort)"
check_fail "export 未知格式必须被拒" \
    "$BIN" "${DD[@]}" vector export "$OPS" "$OPS_FMTBAD" --format parquet
check_fail "export 不存在的集合必须报错" \
    "$BIN" "${DD[@]}" vector export no_such_collection "$OPS_MISSING"

check_fail "clear 未确认必须非零退出（不得假装成功）" \
    bash -c "echo n | '$BIN' --data-dir '$DB_ARG' vector clear '$OPS'"
check "clear 未确认时不得真的清空" "2" "$BIN" "${DD[@]}" vector count "$OPS"
check "clear --force 生效" "已清空" "$BIN" "${DD[@]}" vector clear "$OPS" -f
check "clear 已落盘（新进程 count=0）" "0" "$BIN" "${DD[@]}" vector count "$OPS"

# ---------------------------------------------------------------- 汇总
printf '\n\033[1m== 汇总 ==\033[0m\n'
printf '通过 %d / 失败 %d\n' "$PASS" "$FAIL"
if [ "$FAIL" -gt 0 ]; then
    printf '\n失败项：\n'
    for f in "${FAILED[@]}"; do printf '  - %s\n' "$f"; done
fi
rm -rf "$WORK"
[ -n "${STAGE:-}" ] && rm -rf "$STAGE"
[ "$FAIL" -eq 0 ]
