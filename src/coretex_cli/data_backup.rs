//! File-level snapshot (backup) and restore for a CoreTexDB data directory.
//!
//! v0.1 的落盘状态很小且自包含：`data/coretex/metadata/metadata.json` 是集合定义（manifest），
//! `data/coretex/store/store-NNNNNN.log` 是向量日志，`data/wal/` 是预写日志。所以一次备份就是
//! **把这些文件完整复制一份**，并附上每个文件的 SHA-256，使备份可被独立校验。
//!
//! **这是冷备份**：只在一个进程独占数据目录时才正确。CLI 是"一条命令一个进程"，
//! 所以不要在 server 运行时执行备份——那样可能复制到写了一半的日志。
//! （即便复制到截断的尾部，FileStorage 重放时会物理截断，不会读回垃圾；但会丢最后几条。）
//!
//! 之前 `coretex backup` 只是 `create_dir_all` 一个空目录然后打印「✓ Backup created」，
//! 而 `coretex restore` 只打印一行字——**备份是假的**。本模块把它做成真的可验证备份。

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// 组成持久化状态的一级目录（相对安装根）。只备份这些，绝不递归整个安装根，
/// 因此 `.pre-restore-*/` 与 `data/backup/`、`data/logs/`、`data/temp/` 这类
/// 非核心状态永远不会被卷进下一次备份。
const STATEFUL_DIRS: [&str; 2] = ["data/coretex", "data/wal"];

const MANIFEST: &str = "manifest.json";
const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub version: u32,
    /// RFC 3339 时间戳。
    pub created_at: String,
    pub files: Vec<SnapshotFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFile {
    /// 相对安装根的路径，例如 `data/coretex/metadata/metadata.json`。
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

impl SnapshotManifest {
    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.bytes).sum()
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// 收集 `root/<rel>` 下的所有文件，返回 `(绝对路径, 相对路径)` 并保证顺序稳定。
fn collect_files(root: &Path, rel: &str, out: &mut Vec<(PathBuf, String)>) -> Result<(), String> {
    let dir = root.join(rel);
    if !dir.exists() {
        return Ok(());
    }

    let mut entries: Vec<_> = fs::read_dir(&dir)
        .map_err(|e| format!("读取 {} 失败: {e}", dir.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| format!("读取 {} 失败: {e}", dir.display()))?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let child_rel = format!("{}/{}", rel, entry.file_name().to_string_lossy());
        if path.is_dir() {
            collect_files(root, &child_rel, out)?;
        } else {
            out.push((path, child_rel));
        }
    }
    Ok(())
}

/// 把 `data_dir` 的状态完整快照到 `dest`，返回写入的清单。
///
/// 拒绝生成"空备份"：如果没有任何状态文件，直接报错，而不是造一个空目录假装成功。
pub fn create(data_dir: &Path, dest: &Path) -> Result<SnapshotManifest, String> {
    if !data_dir.is_dir() {
        return Err(format!("数据目录不存在: {}", data_dir.display()));
    }

    let mut sources = Vec::new();
    for dir in STATEFUL_DIRS {
        collect_files(data_dir, dir, &mut sources)?;
    }
    if sources.is_empty() {
        return Err(format!(
            "{} 下没有任何状态文件（既无 data/coretex/ 也无 data/wal/），拒绝生成空备份",
            data_dir.display()
        ));
    }

    fs::create_dir_all(dest).map_err(|e| format!("创建 {} 失败: {e}", dest.display()))?;

    let mut files = Vec::with_capacity(sources.len());
    for (src, rel) in &sources {
        let target = dest.join(rel);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("创建 {} 失败: {e}", parent.display()))?;
        }
        let bytes = fs::read(src).map_err(|e| format!("读取 {} 失败: {e}", src.display()))?;
        fs::write(&target, &bytes).map_err(|e| format!("写入 {} 失败: {e}", target.display()))?;
        files.push(SnapshotFile {
            path: rel.clone(),
            bytes: bytes.len() as u64,
            sha256: sha256_hex(&bytes),
        });
    }

    let manifest = SnapshotManifest {
        version: SNAPSHOT_VERSION,
        created_at: chrono::Utc::now().to_rfc3339(),
        files,
    };
    let json =
        serde_json::to_string_pretty(&manifest).map_err(|e| format!("清单序列化失败: {e}"))?;
    fs::write(dest.join(MANIFEST), json).map_err(|e| format!("写入清单失败: {e}"))?;
    Ok(manifest)
}

pub fn read_manifest(snapshot_dir: &Path) -> Result<SnapshotManifest, String> {
    let path = snapshot_dir.join(MANIFEST);
    let raw = fs::read_to_string(&path)
        .map_err(|e| format!("读取备份清单 {} 失败: {e}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|e| format!("备份清单 {} 不是合法 JSON: {e}", path.display()))
}

/// 逐个文件重新计算 SHA-256。备份若损坏或被人改过，这里就会失败——
/// 而不是等到恢复之后才发现数据是坏的。
pub fn verify(snapshot_dir: &Path) -> Result<SnapshotManifest, String> {
    let manifest = read_manifest(snapshot_dir)?;
    for file in &manifest.files {
        let path = snapshot_dir.join(&file.path);
        let bytes = fs::read(&path).map_err(|e| format!("备份缺少 {}: {e}", file.path))?;
        if bytes.len() as u64 != file.bytes {
            return Err(format!(
                "{} 大小不符：清单 {} 字节，实际 {} 字节",
                file.path,
                file.bytes,
                bytes.len()
            ));
        }
        let actual = sha256_hex(&bytes);
        if actual != file.sha256 {
            return Err(format!(
                "{} 校验和不符：清单 {}，实际 {}",
                file.path, file.sha256, actual
            ));
        }
    }
    Ok(manifest)
}

/// 把快照恢复到 `data_dir`，返回 `(清单, 恢复的文件数)`。
///
/// 现有状态会被**整体移开**到 `data_dir/.pre-restore-<时间戳>/`，而不是原地覆盖：
/// 逐个文件覆盖的话，当前多出来的日志分片会残留下来，重放时把已删除的向量"复活"。
pub fn restore(snapshot_dir: &Path, data_dir: &Path) -> Result<(SnapshotManifest, usize), String> {
    let manifest = verify(snapshot_dir)?;

    if !data_dir.is_dir() {
        return Err(format!("数据目录不存在: {}", data_dir.display()));
    }

    let safety = data_dir.join(format!(
        ".pre-restore-{}",
        chrono::Utc::now().timestamp()
    ));
    fs::create_dir_all(&safety).map_err(|e| format!("创建 {} 失败: {e}", safety.display()))?;

    for dir in STATEFUL_DIRS {
        let current = data_dir.join(dir);
        if current.exists() {
            let keep = safety.join(dir);
            // `dir` is a path like "data/coretex"; rename() does NOT create
            // the target's parent, so without this the move always failed
            // with ENOENT/ERROR_PATH_NOT_FOUND and restore could never run.
            if let Some(parent) = keep.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("创建 {} 失败: {e}", parent.display()))?;
            }
            fs::rename(&current, &keep).map_err(|e| {
                format!("移开现有 {} 失败: {e}", current.display())
            })?;
        }
    }

    let mut restored = 0usize;
    for file in &manifest.files {
        let src = snapshot_dir.join(&file.path);
        let dst = data_dir.join(&file.path);
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("创建 {} 失败: {e}", parent.display()))?;
        }
        fs::copy(&src, &dst).map_err(|e| format!("恢复 {} 失败: {e}", file.path))?;
        restored += 1;
    }

    Ok((manifest, restored))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Regression: restore() used to rename into `.pre-restore-*/data/coretex`
    /// without creating the parent directory first, so the move always failed
    /// with ENOENT and `coretex restore` could never run on any platform.
    #[test]
    fn create_restore_roundtrip_parks_old_state_and_drops_extra_files() {
        let dir = TempDir::new().unwrap();
        let data = dir.path().join("install");

        let coll = data.join("data/coretex/collections/demo");
        fs::create_dir_all(&coll).unwrap();
        fs::write(coll.join("v1.json"), "old").unwrap();
        let wal = data.join("data/wal");
        fs::create_dir_all(&wal).unwrap();
        fs::write(wal.join("wal-000001.log"), "w").unwrap();

        let snap = dir.path().join("snap");
        let manifest = create(&data, &snap).unwrap();
        assert_eq!(manifest.files.len(), 2, "expected 2 state files");

        // Mutate current state: this extra file must NOT survive the restore.
        fs::write(coll.join("v2.json"), "born after backup").unwrap();

        let (m2, restored) = restore(&snap, &data).expect("restore must succeed");
        assert_eq!(restored, 2);
        assert_eq!(m2.files.len(), 2);

        // Snapshot content is back…
        assert_eq!(fs::read_to_string(coll.join("v1.json")).unwrap(), "old");
        // …and the post-backup file is gone (it was parked, not overwritten).
        assert!(!coll.join("v2.json").exists(), "extra file must not survive restore");

        // Old state was parked under `.pre-restore-<ts>/`.
        let safety: Vec<_> = fs::read_dir(&data)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(".pre-restore-"))
            .collect();
        assert_eq!(safety.len(), 1, "expected exactly one .pre-restore dir");
        assert!(safety[0]
            .path()
            .join("data/coretex/collections/demo/v2.json")
            .exists());
    }
}
