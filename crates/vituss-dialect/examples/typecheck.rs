// Scratch: what happens to a MySQL CREATE TABLE rendered for other engines today?
fn main() {
    let my = vituss_dialect::get("mysql").unwrap();
    let sql = "CREATE TABLE user (\
        user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, \
        email VARCHAR(128), \
        payload JSON, \
        created DATETIME, \
        PRIMARY KEY (user_id))";
    let stmt = my.parse_one(sql).unwrap();
    for target in ["mysql", "postgres", "mssql", "sqlite"] {
        let t = vituss_dialect::get(target).unwrap();
        let r = t.render(&stmt, &Default::default(), my.as_ref()).unwrap();
        println!("--- {target} ---\n{}\n", r.sql);
    }
}
