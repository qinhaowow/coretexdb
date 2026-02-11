#!/bin/bash
# CoretexDB 项目名称一致性检查脚本
# 检查所有文件中的项目名称是否统一为 "CoretexDB"

echo "========================================="
echo "  CoretexDB 项目名称一致性检查"
echo "========================================="
echo ""

ERRORS=0
WARNINGS=0

# 1. 检查目录名称
echo "1. 检查目录名称..."
CURRENT_DIR=$(pwd)
if [[ "$CURRENT_DIR" == *"Coretex"* ]]; then
    echo "   ✅ 目录名称正确: Coretex"
else
    echo "   ❌ 目录名称错误: $CURRENT_DIR"
    ((ERRORS++))
fi
echo ""

# 2. 检查 Git 仓库名称
echo "2. 检查 Git 仓库名称..."
REPO_URL=$(git remote get-url origin 2>/dev/null)
if [[ "$REPO_URL" == *"coretex"* ]]; then
    echo "   ✅ Git 仓库名称正确: coretexdb"
    echo "   📍 URL: $REPO_URL"
elif [[ "$REPO_URL" == *"cortex"* ]]; then
    echo "   ❌ Git 仓库名称需要更新: cortexdb → coretexdb"
    echo "   📍 当前 URL: $REPO_URL"
    ((ERRORS++))
else
    echo "   ⚠️  无法识别仓库名称"
    ((WARNINGS++))
fi
echo ""

# 3. 检查 Cargo.toml 包名
echo "3. 检查 Cargo.toml 包名..."
if [ -f "Cargo.toml" ]; then
    CRATE_NAME=$(grep -E "^name\s*=" Cargo.toml | sed 's/.*=\s*"\([^"]*\)"/\1/')
    if [[ "$CRATE_NAME" == *"coretex"* ]]; then
        echo "   ✅ Cargo 包名正确: $CRATE_NAME"
    elif [[ "$CRATE_NAME" == *"cortex"* ]]; then
        echo "   ❌ Cargo 包名需要更新: cortex → coretex"
        echo "   📍 当前包名: $CRATE_NAME"
        ((ERRORS++))
    else
        echo "   ⚠️  Cargo 包名未识别: $CRATE_NAME"
        ((WARNINGS++))
    fi
else
    echo "   ⚠️  未找到 Cargo.toml 文件"
    ((WARNINGS++))
fi
echo ""

# 4. 检查 Cargo.toml 二进制名称
echo "4. 检查 Cargo.toml 二进制名称..."
if [ -f "Cargo.toml" ]; then
    BIN_NAME=$(grep -A1 "\[bin\]" Cargo.toml 2>/dev/null | grep "name" | sed 's/.*=\s*"\([^"]*\)"/\1/' || echo "")
    if [[ "$BIN_NAME" == *"coretex"* ]]; then
        echo "   ✅ 二进制名称正确: $BIN_NAME"
    elif [[ "$BIN_NAME" == *"cortex"* ]]; then
        echo "   ❌ 二进制名称需要更新: cortex → coretex"
        echo "   📍 当前名称: $BIN_NAME"
        ((ERRORS++))
    else
        echo "   ⚠️  二进制名称未设置或未识别"
    fi
fi
echo ""

# 5. 检查 README.md 项目名称
echo "5. 检查 README.md 项目名称..."
if [ -f "README.md" ]; then
    TITLE=$(head -n 1 README.md)
    if [[ "$TITLE" == *"Coretex"* ]]; then
        echo "   ✅ README 标题正确: $TITLE"
    elif [[ "$TITLE" == *"Cortex"* ]]; then
        echo "   ❌ README 标题需要更新: Cortex → Coretex"
        echo "   📍 当前标题: $TITLE"
        ((ERRORS++))
    fi
else
    echo "   ⚠️  未找到 README.md 文件"
    ((WARNINGS++))
fi
echo ""

# 6. 检查 Rust 模块命名
echo "6. 检查 Rust 模块命名..."
CORETEX_MODULES=$(find src -type d -name "coretex_*" 2>/dev/null | wc -l)
CORTEX_MODULES=$(find src -type d -name "cortex_*" 2>/dev/null | wc -l)

if [ "$CORETEX_MODULES" -gt 0 ]; then
    echo "   ✅ 找到 $CORETEX_MODULES 个 coretex_* 模块"
fi

if [ "$CORTEX_MODULES" -gt 0 ]; then
    echo "   ❌ 找到 $CORTEX_MODULES 个 cortex_* 模块需要重命名"
    ((ERRORS++))
fi
echo ""

# 7. 检查主要源码文件中的命名
echo "7. 检查主要源码文件命名..."

# 检查 lib.rs
if [ -f "src/lib.rs" ]; then
    CORETEX_ERRORS=$(grep -c "CortexError\|CortexConfig" src/lib.rs 2>/dev/null || echo "0")
    if [ "$CORETEX_ERRORS" -gt 0 ]; then
        echo "   ❌ lib.rs 中发现 $CORETEX_ERRORS 个旧命名引用"
        ((ERRORS++))
    else
        echo "   ✅ lib.rs 命名正确"
    fi
fi
echo ""

# 8. 检查 Python 包名
echo "8. 检查 Python 包名..."
if [ -d "python" ]; then
    if [ -d "python/coretexdb" ]; then
        echo "   ✅ Python 包目录正确: python/coretexdb"
    elif [ -d "python/cortexdb" ]; then
        echo "   ❌ Python 包目录需要更新: cortexdb → coretexdb"
        ((ERRORS++))
    fi
fi
echo ""

# 9. 检查 SDK 目录名
echo "9. 检查 SDK 目录名..."
SDK_CORETEX=$(find SDK -type d -name "coretexdb" 2>/dev/null | wc -l)
SDK_CORTEX=$(find SDK -type d -name "cortexdb" 2>/dev/null | wc -l)

if [ "$SDK_CORETEX" -gt 0 ]; then
    echo "   ✅ 找到 $SDK_CORETEX 个 coretexdb SDK 目录"
fi

if [ "$SDK_CORTEX" -gt 0 ]; then
    echo "   ❌ 找到 $SDK_CORTEX 个 cortexdb SDK 目录需要重命名"
    ((ERRORS++))
fi
echo ""

# 10. 检查配置文件中的项目名
echo "10. 检查配置文件..."
if [ -f "deploy/helm/coretexdb/Chart.yaml" ]; then
    HELM_NAME=$(grep "^name:" deploy/helm/coretexdb/Chart.yaml | awk '{print $2}')
    if [[ "$HELM_NAME" == *"coretex"* ]]; then
        echo "   ✅ Helm Chart 名称正确: $HELM_NAME"
    elif [[ "$HELM_NAME" == *"cortex"* ]]; then
        echo "   ❌ Helm Chart 名称需要更新: $HELM_NAME → coretexdb"
        ((ERRORS++))
    fi
fi
echo ""

# 总结
echo "========================================="
echo "  检查结果总结"
echo "========================================="
echo "  错误 (Errors): $ERRORS"
echo "  警告 (Warnings): $WARNINGS"
echo ""

if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo "  ✅ 所有检查通过！项目名称一致。"
    echo ""
    echo "  可以安全提交和推送。"
    exit 0
elif [ $ERRORS -eq 0 ]; then
    echo "  ⚠️  有一些警告，但可以继续。"
    echo ""
    echo "  建议在有空时修复警告。"
    exit 0
else
    echo "  ❌ 发现错误！请修复后再推送。"
    echo ""
    echo "  使用以下命令修复常见问题："
    echo "  - 重命名 cortex_* 模块: find src -type d -name 'cortex_*' -exec rename 's/cortex/coretext/g' {} \;"
    echo "  - 更新 Git 远程: git remote set-url origin https://github.com/qinhaowow/coretexdb.git"
    echo ""
    echo "  修复后请重新运行此脚本检查。"
    exit 1
fi
