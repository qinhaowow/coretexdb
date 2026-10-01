//! B1 — C FFI 面：按 `include/coretexdb.h` 声明的**真实 ABI 契约**驱动
//! `src/coretex_ffi.rs`（裸指针、NUL 结尾字符串、JSON out 参数、
//! 线程局部 last_error），因为 C 调用方永远看不到安全 Rust API。
//!
//! `header_declares_exactly_the_rust_ffi_surface` 守住手写头文件与 Rust
//! 定义不漂移；`scripts/build_ffi_example.sh` 另行用 cc 真编译 C 示例。

use coretexdb::coretex_ffi::{
    coretexdb_close, coretexdb_count, coretexdb_create_collection, coretexdb_delete_collection,
    coretexdb_delete_vector, coretexdb_free_string, coretexdb_hybrid_search,
    coretexdb_insert_vector, coretexdb_last_error, coretexdb_list_collections, coretexdb_open,
    coretexdb_search, CoreTexDbHandle, CORETEXDB_ERR_ALREADY_EXISTS, CORETEXDB_ERR_DIMENSION,
    CORETEXDB_ERR_INVALID_ARG, CORETEXDB_ERR_JSON, CORETEXDB_ERR_NOT_FOUND, CORETEXDB_OK,
};
use std::collections::BTreeSet;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

/// RAII：测试尾部自动 close。
struct Handle(*mut CoreTexDbHandle);

impl Drop for Handle {
    fn drop(&mut self) {
        unsafe { coretexdb_close(self.0) };
    }
}

fn cstr(s: &str) -> CString {
    CString::new(s).expect("no interior NUL in test literals")
}

fn last_error() -> String {
    let p = coretexdb_last_error();
    assert!(!p.is_null(), "last_error must never be null");
    unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

fn open(dir: &std::path::Path) -> Handle {
    let path = cstr(dir.to_str().unwrap());
    let mut out: *mut CoreTexDbHandle = std::ptr::null_mut();
    let rc = unsafe { coretexdb_open(path.as_ptr(), &mut out) };
    assert_eq!(rc, CORETEXDB_OK, "open failed: {}", last_error());
    assert!(!out.is_null(), "open must publish a handle on success");
    Handle(out)
}

fn create(h: *mut CoreTexDbHandle, name: &str, dim: u32, metric: &str) -> i32 {
    let (n, m) = (cstr(name), cstr(metric));
    unsafe { coretexdb_create_collection(h, n.as_ptr(), dim, m.as_ptr()) }
}

fn delete_collection(h: *mut CoreTexDbHandle, name: &str) -> i32 {
    let n = cstr(name);
    unsafe { coretexdb_delete_collection(h, n.as_ptr()) }
}

fn insert(
    h: *mut CoreTexDbHandle,
    collection: &str,
    id: &str,
    data: &[f32],
    metadata: Option<&str>,
) -> i32 {
    let (col, id) = (cstr(collection), cstr(id));
    let meta = metadata.map(cstr);
    unsafe {
        coretexdb_insert_vector(
            h,
            col.as_ptr(),
            id.as_ptr(),
            data.as_ptr(),
            data.len() as u32,
            meta.as_ref().map_or(std::ptr::null(), |m| m.as_ptr()),
        )
    }
}

fn delete_vector(h: *mut CoreTexDbHandle, collection: &str, id: &str) -> i32 {
    let (col, id) = (cstr(collection), cstr(id));
    unsafe { coretexdb_delete_vector(h, col.as_ptr(), id.as_ptr()) }
}

fn count(h: *mut CoreTexDbHandle, collection: &str) -> (i32, u64) {
    let col = cstr(collection);
    let mut n: u64 = 0;
    let rc = unsafe { coretexdb_count(h, col.as_ptr(), &mut n) };
    (rc, n)
}

/// 取走 out_json（JSON 文本 → 值），按契约 free 掉 C 侧内存。
fn take_json(out: &mut *mut c_char) -> serde_json::Value {
    if out.is_null() {
        return serde_json::Value::Null;
    }
    let s = unsafe { CStr::from_ptr(*out) }.to_string_lossy().into_owned();
    unsafe { coretexdb_free_string(*out) };
    *out = std::ptr::null_mut();
    if s.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&s).unwrap_or_else(|e| panic!("out JSON must parse ({e}): {s}"))
    }
}

fn search_json(
    h: *mut CoreTexDbHandle,
    collection: &str,
    query: &[f32],
    k: u32,
    filter: Option<&str>,
) -> (i32, serde_json::Value) {
    let col = cstr(collection);
    let f = filter.map(cstr);
    let mut out: *mut c_char = std::ptr::null_mut();
    let rc = unsafe {
        coretexdb_search(
            h,
            col.as_ptr(),
            query.as_ptr(),
            query.len() as u32,
            k,
            f.as_ref().map_or(std::ptr::null(), |f| f.as_ptr()),
            &mut out,
        )
    };
    (rc, take_json(&mut out))
}

#[allow(clippy::too_many_arguments)]
fn hybrid_json(
    h: *mut CoreTexDbHandle,
    collection: &str,
    vector: Option<&[f32]>,
    text: Option<&str>,
    k: u32,
    filter: Option<&str>,
    text_field: Option<&str>,
) -> (i32, serde_json::Value) {
    let col = cstr(collection);
    let t = text.map(cstr);
    let f = filter.map(cstr);
    let field = text_field.map(cstr);
    let (vptr, vlen) = match vector {
        Some(v) => (v.as_ptr(), v.len() as u32),
        None => (std::ptr::null(), 0u32),
    };
    let mut out: *mut c_char = std::ptr::null_mut();
    let rc = unsafe {
        coretexdb_hybrid_search(
            h,
            col.as_ptr(),
            vptr,
            vlen,
            t.as_ref().map_or(std::ptr::null(), |t| t.as_ptr()),
            k,
            f.as_ref().map_or(std::ptr::null(), |f| f.as_ptr()),
            field
                .as_ref()
                .map_or(std::ptr::null(), |fld| fld.as_ptr()),
            &mut out,
        )
    };
    (rc, take_json(&mut out))
}

fn list(h: *mut CoreTexDbHandle) -> serde_json::Value {
    let mut out: *mut c_char = std::ptr::null_mut();
    let rc = unsafe { coretexdb_list_collections(h, &mut out) };
    assert_eq!(rc, CORETEXDB_OK, "list failed: {}", last_error());
    take_json(&mut out)
}

/// 向量 `[i,1,0,0]` 欧氏距离随 i 增大，最近邻即最小 i（与 B2a 同几何）。
fn row(i: f32, text: &str, n: u32) -> (String, Vec<f32>, serde_json::Value) {
    (
        format!("v{n}"),
        vec![i, 1.0, 0.0, 0.0],
        serde_json::json!({ "text": text, "n": n }),
    )
}

#[test]
fn version_matches_cargo_package() {
    let v = unsafe { CStr::from_ptr(coretexdb::coretexdb_version()) }
        .to_str()
        .unwrap();
    assert_eq!(v, env!("CARGO_PKG_VERSION"));
}

#[test]
fn null_and_malformed_arguments_are_rejected_without_crashing() {
    let dir = tempfile::tempdir().unwrap();
    let h = open(dir.path());
    let name = cstr("c");

    // 空句柄：所有以 handle 开头的调用都必须报 INVALID，而不是解引用崩溃。
    let rc =
        unsafe { coretexdb_create_collection(std::ptr::null_mut(), name.as_ptr(), 4, name.as_ptr()) };
    assert_eq!(rc, CORETEXDB_ERR_INVALID_ARG);
    assert!(!last_error().is_empty(), "失败必须留下可读消息");

    let mut n = 0u64;
    let rc = unsafe { coretexdb_count(std::ptr::null_mut(), name.as_ptr(), &mut n) };
    assert_eq!(rc, CORETEXDB_ERR_INVALID_ARG);

    // open 的三条防线：out 为空 / path 为空串 / path 为 NULL。
    let p = cstr("/tmp/coretexdb-ffi-never-created");
    let rc = unsafe { coretexdb_open(p.as_ptr(), std::ptr::null_mut()) };
    assert_eq!(rc, CORETEXDB_ERR_INVALID_ARG);

    let mut out: *mut CoreTexDbHandle = std::ptr::null_mut();
    let rc = unsafe { coretexdb_open(std::ptr::null(), &mut out) };
    assert_eq!(rc, CORETEXDB_ERR_INVALID_ARG);
    assert!(out.is_null(), "失败的 open 不得吐出半吊子句柄");

    let empty = cstr("");
    let mut out2: *mut CoreTexDbHandle = std::ptr::null_mut();
    let rc = unsafe { coretexdb_open(empty.as_ptr(), &mut out2) };
    assert_eq!(rc, CORETEXDB_ERR_INVALID_ARG);
    assert!(out2.is_null());

    // search：query 裸指针为 NULL（即便 len>0）必须拒绝。
    let col = cstr("c");
    let mut json: *mut c_char = std::ptr::null_mut();
    let rc = unsafe {
        coretexdb_search(h.0, col.as_ptr(), std::ptr::null(), 4, 1, std::ptr::null(), &mut json)
    };
    assert_eq!(rc, CORETEXDB_ERR_INVALID_ARG);
    assert!(json.is_null());

    // 成功调用必须清空 last_error（否则陈旧消息会被误读）。
    assert_eq!(create(h.0, "ok", 4, "euclidean"), CORETEXDB_OK, "{}", last_error());
    assert!(last_error().is_empty(), "成功之后 last_error 应为空");
}

#[test]
fn roundtrip_create_insert_search_count_and_filter() {
    let dir = tempfile::tempdir().unwrap();
    let h = open(dir.path());

    assert_eq!(create(h.0, "docs", 4, "euclidean"), CORETEXDB_OK, "{}", last_error());
    assert_eq!(create(h.0, "docs", 4, "euclidean"), CORETEXDB_ERR_ALREADY_EXISTS);
    assert_eq!(create(h.0, "zero", 0, "euclidean"), CORETEXDB_ERR_INVALID_ARG);

    let (a, va, ma) = row(0.0, "alpha release", 0);
    let (b, vb, mb) = row(1.0, "beta notes", 1);
    let (c, vc, mc) = row(2.0, "gamma notes", 2);
    assert_eq!(
        insert(h.0, "docs", &a, &va, Some(&ma.to_string())),
        CORETEXDB_OK,
        "{}",
        last_error()
    );
    assert_eq!(insert(h.0, "docs", &b, &vb, Some(&mb.to_string())), CORETEXDB_OK);
    assert_eq!(insert(h.0, "docs", &c, &vc, Some(&mc.to_string())), CORETEXDB_OK);

    // 维度不符 → DIMENSION（由库内校验产生，非 FFI 层伪造）。
    assert_eq!(insert(h.0, "docs", "bad", &[1.0, 2.0], None), CORETEXDB_ERR_DIMENSION);

    let (rc, n) = count(h.0, "docs");
    assert_eq!(rc, CORETEXDB_OK, "{}", last_error());
    assert_eq!(n, 3);

    // 最近邻：euclidean 下 query [0,1,0,0] 最近的是 v0，距离递增。
    let (rc, hits) = search_json(h.0, "docs", &[0.0, 1.0, 0.0, 0.0], 3, None);
    assert_eq!(rc, CORETEXDB_OK, "{}", last_error());
    let hits = hits.as_array().expect("array of hits").clone();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0]["id"], "v0");
    assert!(hits[0]["distance"].as_f64().unwrap() < hits[1]["distance"].as_f64().unwrap());
    assert!(hits[1]["distance"].as_f64().unwrap() < hits[2]["distance"].as_f64().unwrap());

    // 过滤：操作符格式与 REST/库内一致 {"n":{"$gte":1}}。
    let (rc, filtered) = search_json(h.0, "docs", &[0.0, 1.0, 0.0, 0.0], 3, Some(r#"{"n":{"$gte":1}}"#));
    assert_eq!(rc, CORETEXDB_OK, "{}", last_error());
    let ids: Vec<&str> = filtered
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["v1", "v2"]);

    // 未知集合 / 坏 JSON 过滤 / 删不存在的 id。
    let (rc, _) = search_json(h.0, "missing", &[0.0, 1.0, 0.0, 0.0], 1, None);
    assert_eq!(rc, CORETEXDB_ERR_NOT_FOUND, "未知集合必须 NOT_FOUND");
    let (rc, _) = search_json(h.0, "docs", &[0.0, 1.0, 0.0, 0.0], 1, Some("{oops"));
    assert_eq!(rc, CORETEXDB_ERR_JSON, "坏 JSON 必须走 JSON 状态码");
    assert_eq!(delete_vector(h.0, "docs", "ghost"), CORETEXDB_ERR_NOT_FOUND);
    assert_eq!(delete_vector(h.0, "docs", &c), CORETEXDB_OK);

    let (rc, n) = count(h.0, "docs");
    assert_eq!((rc, n), (CORETEXDB_OK, 2), "删除后计数必须回落");
}

#[test]
fn hybrid_search_fuses_both_sides_through_the_abi() {
    let dir = tempfile::tempdir().unwrap();
    let h = open(dir.path());
    assert_eq!(create(h.0, "docs", 4, "euclidean"), CORETEXDB_OK, "{}", last_error());

    for (i, text, n) in [(0.0f32, "note zero", 0u32), (1.0, "note one", 1), (3.0, "alpha release", 3)] {
        let (id, v, meta) = row(i, text, n);
        assert_eq!(insert(h.0, "docs", &id, &v, Some(&meta.to_string())), CORETEXDB_OK);
    }
    let query = [0.0f32, 1.0, 0.0, 0.0];

    // 双侧：向量路命中 v0/v1/v3，文本路命中 v3 → v3 双来源必须排第一。
    let (rc, hits) = hybrid_json(h.0, "docs", Some(&query), Some("alpha"), 5, None, None);
    assert_eq!(rc, CORETEXDB_OK, "{}", last_error());
    let hits = hits.as_array().expect("array of hits").clone();
    assert_eq!(hits.len(), 3, "双路并集应为三篇");
    assert_eq!(hits[0]["id"], "v3");
    let mut sources: Vec<&str> = hits[0]["sources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect();
    sources.sort_unstable();
    assert_eq!(sources, ["text", "vector"], "双命中必须标注双来源");

    // 仅文本（vector=NULL）。
    let (rc, hits) = hybrid_json(h.0, "docs", None, Some("alpha"), 5, None, None);
    assert_eq!(rc, CORETEXDB_OK, "{}", last_error());
    let hits = hits.as_array().unwrap().clone();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["id"], "v3");
    assert_eq!(hits[0]["sources"][0], "text");

    // 仅向量（空白文本按“缺省”处理，与 Rust 语义一致）。
    let (rc, hits) = hybrid_json(h.0, "docs", Some(&query), Some("   "), 5, None, None);
    assert_eq!(rc, CORETEXDB_OK, "{}", last_error());
    let hits = hits.as_array().unwrap().clone();
    assert_eq!(hits.len(), 3);
    assert!(hits.iter().all(|x| x["sources"] == serde_json::json!(["vector"])));

    // 过滤两侧生效：只留 n=0 → 文本命中的 v3 被剔除，仅剩向量侧 v0。
    let (rc, hits) = hybrid_json(h.0, "docs", Some(&query), Some("alpha"), 5, Some(r#"{"n":0}"#), None);
    assert_eq!(rc, CORETEXDB_OK, "{}", last_error());
    let hits = hits.as_array().unwrap().clone();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["id"], "v0");
    assert_eq!(hits[0]["sources"][0], "vector");

    // 双侧皆缺 → INVALID_ARG（C 层比 Rust 层更严格的防呆）。
    let (rc, _) = hybrid_json(h.0, "docs", None, None, 5, None, None);
    assert_eq!(rc, CORETEXDB_ERR_INVALID_ARG);

    // k=0 → 合法空结果。
    let (rc, hits) = hybrid_json(h.0, "docs", Some(&query), Some("alpha"), 0, None, None);
    assert_eq!(rc, CORETEXDB_OK);
    assert_eq!(hits.as_array().unwrap().len(), 0);

    // 自定义文本字段：把 n 当文本字段查不到东西，但不应报错。
    let (rc, _) = hybrid_json(h.0, "docs", None, Some("alpha"), 5, None, Some("no_such_field"));
    assert_eq!(rc, CORETEXDB_OK, "{}", last_error());
}

#[test]
fn list_and_delete_collections_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let h = open(dir.path());
    assert_eq!(create(h.0, "a", 2, "euclidean"), CORETEXDB_OK);
    assert_eq!(create(h.0, "b", 2, "euclidean"), CORETEXDB_OK);

    let names = list(h.0);
    let mut names: Vec<&str> = names
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["a", "b"]);

    assert_eq!(delete_collection(h.0, "a"), CORETEXDB_OK);
    assert_eq!(delete_collection(h.0, "a"), CORETEXDB_ERR_NOT_FOUND);
    assert_eq!(list(h.0), serde_json::json!(["b"]));
}

#[test]
fn two_handles_are_isolated() {
    let da = tempfile::tempdir().unwrap();
    let db = tempfile::tempdir().unwrap();
    let ha = open(da.path());
    let hb = open(db.path());

    assert_eq!(create(ha.0, "only_a", 2, "euclidean"), CORETEXDB_OK);
    let (rc, _) = count(hb.0, "only_a");
    assert_eq!(rc, CORETEXDB_ERR_NOT_FOUND, "句柄之间集合必须互相隔离");
    assert_eq!(list(hb.0), serde_json::json!([]));
}

/// 手写头文件与 Rust FFI 面必须**恰好**一致：头里声明的每个
/// `coretexdb_*(` 都要有 `extern "C" fn` 定义，反之亦然；版本宏必须
/// 跟随 `Cargo.toml`。
#[test]
fn header_declares_exactly_the_rust_ffi_surface() {
    let root = env!("CARGO_MANIFEST_DIR");
    let header = std::fs::read_to_string(format!("{root}/include/coretexdb.h")).unwrap();

    // 头文件侧：非注释行里 `coretexdb_*` 后跟 '(' 的即声明。
    let mut declared: BTreeSet<String> = BTreeSet::new();
    for line in header.lines().map(str::trim) {
        if line.starts_with("//") || line.starts_with("/*") || line.starts_with('*') {
            continue;
        }
        let mut from = 0;
        while let Some(rel) = line[from..].find("coretexdb_") {
            let start = from + rel;
            let rest = &line[start..];
            let end = rest
                .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
                .unwrap_or(rest.len());
            if rest[end..].trim_start().starts_with('(') {
                declared.insert(rest[..end].to_string());
            }
            from = start + end;
        }
    }

    // Rust 侧：FFI 的两个家（lib.rs 的 version + coretex_ffi.rs）里
    // `extern "C" fn coretexdb_*` 的定义。
    let mut defined: BTreeSet<String> = BTreeSet::new();
    for file in ["src/lib.rs", "src/coretex_ffi.rs"] {
        let text = std::fs::read_to_string(format!("{root}/{file}")).unwrap();
        let needle = "extern \"C\" fn ";
        let mut from = 0;
        while let Some(rel) = text[from..].find(needle) {
            let start = from + rel + needle.len();
            let rest = &text[start..];
            let end = rest
                .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
                .unwrap_or(rest.len());
            if end > 0 {
                defined.insert(rest[..end].to_string());
            }
            from = start + end.max(1);
        }
    }

    assert!(
        !declared.is_empty(),
        "头文件应至少声明一组函数，解析为空说明格式变了"
    );
    assert_eq!(
        declared, defined,
        "include/coretexdb.h 与 Rust FFI 定义不一致（手写头文件漂移了）"
    );

    // 版本宏跟随 Cargo.toml。
    let mut parts = env!("CARGO_PKG_VERSION").split('.');
    let expect = [
        ("MAJOR", parts.next().unwrap()),
        ("MINOR", parts.next().unwrap()),
        ("PATCH", parts.next().unwrap()),
    ];
    for (label, value) in expect {
        let line = format!("#define CORETEXDB_VERSION_{label} {value}");
        assert!(header.contains(&line), "头文件缺少 `{line}`");
    }
}
