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

  const [maxRows] = await db.query('SELECT MAX(AccessDate) as max_date FROM v_attendance_with_rates');
  const maxDate = maxRows[0]?.max_date ? new Date(maxRows[0].max_date) : new Date();
  const y = maxDate.getFullYear();
  const m = maxDate.getMonth() + 1;
  const monthStart = `${y}-${String(m).padStart(2, '0')}-01`;
  const lastDay = new Date(y, m, 0).getDate();
  const monthEnd = `${y}-${String(m).padStart(2, '0')}-${String(lastDay).padStart(2, '0')}`;

  const api = await fetch('http://localhost:3004/api/dashboard/stats').then((r) => r.json());

  const [[p], [s], [a], [sh], [e], [d]] = await Promise.all([
    db.query('SELECT COUNT(*) as count FROM Parents'),
    db.query('SELECT COUNT(*) as count FROM Students'),
    db.query('SELECT COUNT(DISTINCT ParentID) as count FROM v_attendance_with_rates WHERE AccessDate BETWEEN ? AND ?', [monthStart, monthEnd]),
    db.query('SELECT COUNT(*) as count FROM shifts WHERE status = 1'),
    db.query('SELECT COALESCE(SUM(money_earned_rwf + fee_earned_rwf), 0) as total FROM v_attendance_with_rates WHERE AccessDate BETWEEN ? AND ?', [monthStart, monthEnd]),
    db.query('SELECT COUNT(*) as active FROM devices WHERE status = 1')
  ]);

  const [trendRows] = await db.query(
    'SELECT DATE(AccessDate) as date, COALESCE(SUM(money_earned_rwf + fee_earned_rwf), 0) as daily_earnings FROM v_attendance_with_rates WHERE AccessDate BETWEEN ? AND ? GROUP BY DATE(AccessDate) ORDER BY date',
    [monthStart, monthEnd]
  );

  const dbData = {
    monthlyRange: { startDate: monthStart, endDate: monthEnd },
    totalParents: Number(p[0].count),
    totalStudents: Number(s[0].count),
    monthAttendance: Number(a[0].count),
    activeShifts: Number(sh[0].count),
    monthEarnings: Number(e[0].total),
    activeDevices: Number(d[0].active),
    trendPoints: trendRows.length
  };

  const apiData = {
    monthlyRange: api.monthlyRange,
    totalParents: Number(api.totalParents || 0),
    totalStudents: Number(api.totalStudents || 0),
    monthAttendance: Number(api.monthAttendance || 0),
    activeShifts: Number(api.activeShifts || 0),
    monthEarnings: Number(api.monthEarnings || 0),
    activeDevices: Number(api.activeDevices || 0),
    trendPoints: (api.monthlyTrend || []).length
  };

  const matches = {
    range: apiData.monthlyRange?.startDate === dbData.monthlyRange.startDate && apiData.monthlyRange?.endDate === dbData.monthlyRange.endDate,
    totalParents: apiData.totalParents === dbData.totalParents,
    totalStudents: apiData.totalStudents === dbData.totalStudents,
    monthAttendance: apiData.monthAttendance === dbData.monthAttendance,
    activeShifts: apiData.activeShifts === dbData.activeShifts,
    monthEarnings: apiData.monthEarnings === dbData.monthEarnings,
    activeDevices: apiData.activeDevices === dbData.activeDevices,
    trendPoints: apiData.trendPoints === dbData.trendPoints
  };

  console.log(JSON.stringify({ apiData, dbData, matches }, null, 2));
  await db.end();
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
