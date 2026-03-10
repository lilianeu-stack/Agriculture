const mysql = require('mysql2/promise');

(async () => {
  const db = await mysql.createPool({
    host: '192.171.2.70',
    user: 'hikuser',
    password: 'Hope@2025!',
    database: 'agliculture_att',
    waitForConnections: true,
    connectionLimit: 5
  });

  const [tables] = await db.query("SHOW TABLES");
  const tableNames = tables.map((r) => Object.values(r)[0]);

  const [candidateColumns] = await db.query(`
    SELECT TABLE_NAME, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'agliculture_att'
      AND (
        TABLE_NAME LIKE '%fee%' OR TABLE_NAME LIKE '%payment%' OR TABLE_NAME LIKE '%school%'
        OR COLUMN_NAME LIKE '%fee%' OR COLUMN_NAME LIKE '%payment%' OR COLUMN_NAME LIKE '%amount%'
      )
    ORDER BY TABLE_NAME, ORDINAL_POSITION
  `);

  const candidateTables = [...new Set(candidateColumns.map((r) => r.TABLE_NAME))];
  const counts = {};
  for (const t of candidateTables) {
    try {
      const [rows] = await db.query(`SELECT COUNT(*) as c FROM \`${t}\``);
      counts[t] = rows[0].c;
    } catch {
      counts[t] = 'unreadable';
    }
  }

  console.log(JSON.stringify({
    allTablesCount: tableNames.length,
    candidateTables,
    candidateTableCounts: counts,
    sampleColumns: candidateColumns.slice(0, 120)
  }, null, 2));

  await db.end();
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
